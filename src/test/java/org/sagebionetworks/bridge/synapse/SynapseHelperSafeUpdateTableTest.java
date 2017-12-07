package org.sagebionetworks.bridge.synapse;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableList;
import org.mockito.ArgumentCaptor;
import org.sagebionetworks.repo.model.table.ColumnChange;
import org.sagebionetworks.repo.model.table.ColumnModel;
import org.sagebionetworks.repo.model.table.ColumnType;
import org.sagebionetworks.repo.model.table.TableSchemaChangeRequest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.exceptions.BridgeSynapseNonRetryableException;

public class SynapseHelperSafeUpdateTableTest {
    private static final String SYNAPSE_TABLE_ID = "dummy-synapse-table-id";

    private SynapseHelper synapseHelper;

    @BeforeMethod
    public void before() throws Exception {
        // Spy SynapseHelper. This way, we can test the logic in SynapseHelper without being tightly coupled to the
        // implementations of get column, create column, or update table.
        // Spied methods return null as a placeholder. If tests need to return specific values, that test needs to set
        // up the mock.
        synapseHelper = spy(new SynapseHelper());
        doReturn(null).when(synapseHelper).createColumnModelsWithRetry(any());
        doReturn(null).when(synapseHelper).updateTableColumns(any(), any());

        // Create the "old column list" that we work with.
        List<ColumnModel> oldColumnList = ImmutableList.of(makeColumn("foo", "foo-id"),
                makeColumn("bar", "bar-id"));
        doReturn(oldColumnList).when(synapseHelper).getColumnModelsForTableWithRetry(SYNAPSE_TABLE_ID);
    }

    @Test(expectedExceptions = BridgeSynapseNonRetryableException.class, expectedExceptionsMessageRegExp =
            "Table has deleted and/or modified columns")
    public void rejectDelete() throws Exception {
        // New column is missing "foo".
        List<ColumnModel> newColumnList = ImmutableList.of(makeColumn("bar", null));
        synapseHelper.safeUpdateTable(SYNAPSE_TABLE_ID, newColumnList);
    }

    @Test(expectedExceptions = BridgeSynapseNonRetryableException.class, expectedExceptionsMessageRegExp =
            "Table has deleted and/or modified columns")
    public void incompatibleTypeChange() throws Exception {
        // New column "foo" is a file handle.
        List<ColumnModel> newColumnList = new ArrayList<>();

        ColumnModel newFooColumn = new ColumnModel();
        newFooColumn.setName("foo");
        newFooColumn.setColumnType(ColumnType.FILEHANDLEID);
        newColumnList.add(newFooColumn);

        newColumnList.add(makeColumn("bar", null));

        synapseHelper.safeUpdateTable(SYNAPSE_TABLE_ID, newColumnList);
    }

    @Test(expectedExceptions = BridgeSynapseNonRetryableException.class, expectedExceptionsMessageRegExp =
            "Table has deleted and/or modified columns")
    public void incompatibleLengthChange() throws Exception {
        // New column "foo" length decreased to 24
        List<ColumnModel> newColumnList = new ArrayList<>();

        ColumnModel newFooColumn = new ColumnModel();
        newFooColumn.setName("foo");
        newFooColumn.setColumnType(ColumnType.STRING);
        newFooColumn.setMaximumSize(24L);
        newColumnList.add(newFooColumn);

        newColumnList.add(makeColumn("bar", null));

        synapseHelper.safeUpdateTable(SYNAPSE_TABLE_ID, newColumnList);
    }

    @Test
    public void dontUpdateIfNoAddedOrModifiedColumns() throws Exception {
        // Swap foo and bar. No update.
        List<ColumnModel> newColumnList = ImmutableList.of(makeColumn("bar", null),
                makeColumn("foo", null));
        synapseHelper.safeUpdateTable(SYNAPSE_TABLE_ID, newColumnList);

        // Verify dependent calls (or lack thereof)
        verify(synapseHelper, never()).createColumnModelsWithRetry(any());
        verify(synapseHelper, never()).updateTableColumns(any(), any());
    }

    @Test
    public void addAndSwapColumns() throws Exception {
        // Swap foo and bar, and insert baz.
        List<ColumnModel> newColumnList = ImmutableList.of(makeColumn("bar", null),
                makeColumn("foo", null), makeColumn("baz", null));

        // We create these columns (which are the same, but have column IDs).
        List<ColumnModel> createdColumnList = ImmutableList.of(makeColumn("bar", "bar-id"),
                makeColumn("foo", "foo-id"), makeColumn("baz", "baz-id"));
        doReturn(createdColumnList).when(synapseHelper).createColumnModelsWithRetry(newColumnList);

        // Execute
        synapseHelper.safeUpdateTable(SYNAPSE_TABLE_ID, newColumnList);

        // We call createColumnModels() with all of the columns.
        verify(synapseHelper).createColumnModelsWithRetry(newColumnList);

        // Verify update table call.
        ArgumentCaptor<TableSchemaChangeRequest> requestCaptor = ArgumentCaptor.forClass(
                TableSchemaChangeRequest.class);
        verify(synapseHelper).updateTableColumns(requestCaptor.capture(), eq(SYNAPSE_TABLE_ID));

        TableSchemaChangeRequest request = requestCaptor.getValue();
        assertEquals(request.getEntityId(), SYNAPSE_TABLE_ID);
        assertEquals(request.getOrderedColumnIds(), ImmutableList.of("bar-id", "foo-id", "baz-id"));

        List<ColumnChange> changeList = request.getChanges();
        assertEquals(changeList.size(), 1);
        assertNull(changeList.get(0).getOldColumnId());
        assertEquals(changeList.get(0).getNewColumnId(), "baz-id");
    }

    @Test
    public void compatibleTypeChange() throws Exception {
        // Old column is an int.
        ColumnModel oldColumn = new ColumnModel();
        oldColumn.setName("my-col");
        oldColumn.setId("old-col-id");
        oldColumn.setColumnType(ColumnType.INTEGER);
        List<ColumnModel> oldColumnList = ImmutableList.of(oldColumn);
        doReturn(oldColumnList).when(synapseHelper).getColumnModelsForTableWithRetry(SYNAPSE_TABLE_ID);

        // New column is a string.
        List<ColumnModel> newColumnList = ImmutableList.of(makeColumn("my-col", null));
        List<ColumnModel> createdColumnList = ImmutableList.of(makeColumn("my-col", "new-col-id"));
        doReturn(createdColumnList).when(synapseHelper).createColumnModelsWithRetry(newColumnList);

        // Execute
        synapseHelper.safeUpdateTable(SYNAPSE_TABLE_ID, newColumnList);

        // We call createColumnModels() with all of the columns.
        verify(synapseHelper).createColumnModelsWithRetry(newColumnList);

        // Verify update table call.
        ArgumentCaptor<TableSchemaChangeRequest> requestCaptor = ArgumentCaptor.forClass(
                TableSchemaChangeRequest.class);
        verify(synapseHelper).updateTableColumns(requestCaptor.capture(), eq(SYNAPSE_TABLE_ID));

        TableSchemaChangeRequest request = requestCaptor.getValue();
        assertEquals(request.getEntityId(), SYNAPSE_TABLE_ID);
        assertEquals(request.getOrderedColumnIds(), ImmutableList.of("new-col-id"));

        List<ColumnChange> changeList = request.getChanges();
        assertEquals(changeList.size(), 1);
        assertEquals(changeList.get(0).getOldColumnId(), "old-col-id");
        assertEquals(changeList.get(0).getNewColumnId(), "new-col-id");
    }

    @Test
    public void compatibleLengthChange() throws Exception {
        // Old column is a string of length 24, which will be increased to 100.
        ColumnModel oldColumn = new ColumnModel();
        oldColumn.setName("my-col");
        oldColumn.setId("old-col-id");
        oldColumn.setColumnType(ColumnType.STRING);
        oldColumn.setMaximumSize(24L);
        List<ColumnModel> oldColumnList = ImmutableList.of(oldColumn);
        doReturn(oldColumnList).when(synapseHelper).getColumnModelsForTableWithRetry(SYNAPSE_TABLE_ID);

        // New column is a string of length 100.
        List<ColumnModel> newColumnList = ImmutableList.of(makeColumn("my-col", null));
        List<ColumnModel> createdColumnList = ImmutableList.of(makeColumn("my-col", "new-col-id"));
        doReturn(createdColumnList).when(synapseHelper).createColumnModelsWithRetry(newColumnList);

        // Execute
        synapseHelper.safeUpdateTable(SYNAPSE_TABLE_ID, newColumnList);

        // We call createColumnModels() with all of the columns.
        verify(synapseHelper).createColumnModelsWithRetry(newColumnList);

        // Verify update table call.
        ArgumentCaptor<TableSchemaChangeRequest> requestCaptor = ArgumentCaptor.forClass(
                TableSchemaChangeRequest.class);
        verify(synapseHelper).updateTableColumns(requestCaptor.capture(), eq(SYNAPSE_TABLE_ID));

        TableSchemaChangeRequest request = requestCaptor.getValue();
        assertEquals(request.getEntityId(), SYNAPSE_TABLE_ID);
        assertEquals(request.getOrderedColumnIds(), ImmutableList.of("new-col-id"));

        List<ColumnChange> changeList = request.getChanges();
        assertEquals(changeList.size(), 1);
        assertEquals(changeList.get(0).getOldColumnId(), "old-col-id");
        assertEquals(changeList.get(0).getNewColumnId(), "new-col-id");
    }

    private static ColumnModel makeColumn(String name, String id) {
        ColumnModel col = new ColumnModel();
        col.setName(name);
        col.setId(id);
        col.setColumnType(ColumnType.STRING);
        col.setMaximumSize(100L);
        return col;
    }
}
