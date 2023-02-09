package org.sagebionetworks.bridge.synapse;

import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertSame;

import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.exceptions.SynapseResultNotReadyException;
import org.sagebionetworks.repo.model.table.AppendableRowSet;
import org.sagebionetworks.repo.model.table.PartialRowSet;
import org.sagebionetworks.repo.model.table.RowReferenceSet;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.exceptions.BridgeSynapseException;

@SuppressWarnings("unchecked")
public class SynapseHelperAppendRowsTest {
    private static final AppendableRowSet APPENDABLE_ROW_SET = new PartialRowSet();
    private static final String JOB_TOKEN = "job-token";
    private static final RowReferenceSet ROW_REFERENCE_SET = new RowReferenceSet();
    private static final String TABLE_ID = "table-id";

    @Mock
    private SynapseClient mockSynapseClient;

    @InjectMocks
    private SynapseHelper synapseHelper;

    @BeforeMethod
    public void setup() throws Exception {
        MockitoAnnotations.initMocks(this);

        // Set async loop and rate limits to make tests more reasonable.
        synapseHelper.setAsyncGetBackoffPlan(new int[] { 0, 0 });
        synapseHelper.setRateLimit(1000);
        synapseHelper.setGetColumnModelsRateLimit(1000);

        // Mock start call, which is always the same.
        when(mockSynapseClient.appendRowSetToTableStart(same(APPENDABLE_ROW_SET), eq(TABLE_ID))).thenReturn(JOB_TOKEN);
    }

    @Test
    public void normalCase() throws Exception {
        // Mock Synapse Client. First loop not ready. Second loop has results.
        when(mockSynapseClient.appendRowSetToTableGet(JOB_TOKEN, TABLE_ID))
                .thenThrow(SynapseResultNotReadyException.class).thenReturn(ROW_REFERENCE_SET);

        // Execute and validate.
        RowReferenceSet result = synapseHelper.appendRowsToTable(APPENDABLE_ROW_SET, TABLE_ID);
        assertSame(result, ROW_REFERENCE_SET);
    }

    @Test(expectedExceptions = BridgeSynapseException.class)
    public void timeout() throws Exception {
        // Mock Synapse Client. Result is always not ready.
        when(mockSynapseClient.appendRowSetToTableGet(JOB_TOKEN, TABLE_ID))
                .thenThrow(SynapseResultNotReadyException.class);

        // Execute - throws.
        synapseHelper.appendRowsToTable(APPENDABLE_ROW_SET, TABLE_ID);
    }
}
