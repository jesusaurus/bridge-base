package org.sagebionetworks.bridge.synapse;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.exceptions.SynapseNotFoundException;
import org.sagebionetworks.client.exceptions.SynapseResultNotReadyException;
import org.sagebionetworks.repo.model.AccessControlList;
import org.sagebionetworks.repo.model.Folder;
import org.sagebionetworks.repo.model.ResourceAccess;
import org.sagebionetworks.repo.model.Team;
import org.sagebionetworks.repo.model.annotation.v2.Annotations;
import org.sagebionetworks.repo.model.annotation.v2.AnnotationsValue;
import org.sagebionetworks.repo.model.annotation.v2.AnnotationsValueType;
import org.sagebionetworks.repo.model.file.FileHandle;
import org.sagebionetworks.repo.model.file.S3FileHandle;
import org.sagebionetworks.repo.model.project.ExternalS3StorageLocationSetting;
import org.sagebionetworks.repo.model.project.ProjectSetting;
import org.sagebionetworks.repo.model.project.ProjectSettingsType;
import org.sagebionetworks.repo.model.project.UploadDestinationListSetting;
import org.sagebionetworks.repo.model.status.StackStatus;
import org.sagebionetworks.repo.model.status.StatusEnum;
import org.sagebionetworks.repo.model.table.ColumnModel;
import org.sagebionetworks.repo.model.table.ColumnType;
import org.sagebionetworks.repo.model.table.CsvTableDescriptor;
import org.sagebionetworks.repo.model.table.TableEntity;
import org.sagebionetworks.repo.model.table.TableUpdateRequest;
import org.sagebionetworks.repo.model.table.TableUpdateResponse;
import org.sagebionetworks.repo.model.table.UploadToTableResult;
import org.sagebionetworks.repo.model.util.ModelConstants;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.exceptions.BridgeSynapseNonRetryableException;

@SuppressWarnings("unchecked")
public class SynapseHelperTest {
    private static final String ENTITY_NAME_CHILD = "Child Name";
    private static final String ETAG = "dummy etag";
    private static final long STORAGE_LOCATION_ID = 1234L;
    private static final String SYNAPSE_CHILD_ID = "synChild";
    private static final String SYNAPSE_ENTITY_ID = "synEntity";
    private static final String SYNAPSE_PARENT_ID = "synParent";

    @Mock
    private SynapseClient mockSynapseClient;

    // Spy SynapseHelper. This way, we can test the logic in SynapseHelper without being tightly coupled to the
    // implementations of create column, create table, and create ACLs.
    @InjectMocks
    @Spy
    private SynapseHelper synapseHelper;

    @BeforeMethod
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void createTableWithColumnsAndAcls() throws Exception {
        // Set up inputs. This table will have two columns. We pass this straight through to the create column call,
        // and we never look inside, so don't bother actually instantiating the columns.
        List<ColumnModel> columnList = ImmutableList.of(new ColumnModel(), new ColumnModel());

        // mock create column call - We only care about the IDs, so don't bother instantiating the rest.
        ColumnModel createdFooColumn = new ColumnModel();
        createdFooColumn.setId("foo-col-id");

        ColumnModel createdBarColumn = new ColumnModel();
        createdBarColumn.setId("bar-col-id");

        List<ColumnModel> createdColumnList = ImmutableList.of(createdFooColumn, createdBarColumn);

        doReturn(createdColumnList).when(synapseHelper).createColumnModelsWithRetry(columnList);

        // mock create table call - We only care about the table ID, so don't bother instantiating the rest.
        TableEntity createdTable = new TableEntity();
        createdTable.setId("test-table");

        ArgumentCaptor<TableEntity> tableCaptor = ArgumentCaptor.forClass(TableEntity.class);
        doReturn(createdTable).when(synapseHelper).createTableWithRetry(tableCaptor.capture());

        // Mock create ACL call. Even though we don't care about the return value, we have to do it, otherwise it'll
        // call through to the real method. Likewise, for the result, just return a dummy object. We never look at it
        // anyway.
        ArgumentCaptor<AccessControlList> aclCaptor = ArgumentCaptor.forClass(AccessControlList.class);
        doReturn(new AccessControlList()).when(synapseHelper).createAclWithRetry(aclCaptor.capture());

        // execute and validate
        Set<Long> readOnlyPrincipalSet = ImmutableSet.of(1111L, 2222L);
        Set<Long> adminPrincipalSet = ImmutableSet.of(3333L, 4444L);
        String retVal = synapseHelper.createTableWithColumnsAndAcls(columnList, readOnlyPrincipalSet, adminPrincipalSet,
                "test-project", "My Table");
        assertEquals(retVal, "test-table");

        // validate tableCaptor
        TableEntity table = tableCaptor.getValue();
        assertEquals(table.getName(), "My Table");
        assertEquals(table.getParentId(), "test-project");
        assertEquals(table.getColumnIds(), ImmutableList.of("foo-col-id", "bar-col-id"));

        // validate aclCaptor
        AccessControlList acl = aclCaptor.getValue();
        assertEquals(acl.getId(), "test-table");

        Set<ResourceAccess> resourceAccessSet = acl.getResourceAccess();
        assertEquals(resourceAccessSet.size(), 4);
        Map<Long, ResourceAccess> resourceAccessByPrincipalId = Maps.uniqueIndex(resourceAccessSet,
                ResourceAccess::getPrincipalId);
        assertEquals(resourceAccessByPrincipalId.size(), 4);

        assertEquals(resourceAccessByPrincipalId.get(1111L).getAccessType(), SynapseHelper.ACCESS_TYPE_READ);
        assertEquals(resourceAccessByPrincipalId.get(2222L).getAccessType(), SynapseHelper.ACCESS_TYPE_READ);
        assertEquals(resourceAccessByPrincipalId.get(3333L).getAccessType(),
                ModelConstants.ENTITY_ADMIN_ACCESS_PERMISSIONS);
        assertEquals(resourceAccessByPrincipalId.get(4444L).getAccessType(),
                ModelConstants.ENTITY_ADMIN_ACCESS_PERMISSIONS);
    }

    @Test
    public void isCompatibleColumnTypeChanges() throws Exception {
        ColumnModel intColumn = new ColumnModel();
        intColumn.setName("my-column");
        intColumn.setColumnType(ColumnType.INTEGER);

        ColumnModel floatColumn = new ColumnModel();
        floatColumn.setName("my-column");
        floatColumn.setColumnType(ColumnType.DOUBLE);

        assertTrue(SynapseHelper.isCompatibleColumn(intColumn, floatColumn));
        assertFalse(SynapseHelper.isCompatibleColumn(floatColumn, intColumn));
    }

    @DataProvider
    public Object[][] isCompatibleMaxLengthStringToStringDataProvider() {
        // { oldMaxLength, newMaxLength, expected }
        return new Object[][] {
                { 10, 20, true },
                { 20, 10, false },
                { 15, 15, true },
        };
    }

    @Test(dataProvider = "isCompatibleMaxLengthStringToStringDataProvider")
    public void isCompatibleMaxLengthStringToString(long oldMaxLength, long newMaxLength, boolean expected)
            throws Exception {
        ColumnModel oldColumn = new ColumnModel();
        oldColumn.setName("my-string");
        oldColumn.setColumnType(ColumnType.STRING);
        oldColumn.setMaximumSize(oldMaxLength);

        ColumnModel newColumn = new ColumnModel();
        newColumn.setName("my-string");
        newColumn.setColumnType(ColumnType.STRING);
        newColumn.setMaximumSize(newMaxLength);

        assertEquals(SynapseHelper.isCompatibleColumn(oldColumn, newColumn), expected);
    }

    @DataProvider
    public Object[][] isCompatibleMaxLengthWithNonStringsDataProvider() {
        // { oldType, newType, newMaxLength, expected }
        return new Object[][] {
                { ColumnType.INTEGER, ColumnType.INTEGER, null, true },
                { ColumnType.DOUBLE, ColumnType.STRING, 21L, false },
                { ColumnType.DOUBLE, ColumnType.STRING, 23L, true },
                { ColumnType.INTEGER, ColumnType.STRING, 19L, false },
                { ColumnType.INTEGER, ColumnType.STRING, 21L, true },
        };
    }

    @Test(dataProvider = "isCompatibleMaxLengthWithNonStringsDataProvider")
    public void isCompatibleMaxLengthWithNonStrings(ColumnType oldType, ColumnType newType, Long newMaxLength,
            boolean expected) throws Exception {
        ColumnModel oldColumn = new ColumnModel();
        oldColumn.setName("my-column");
        oldColumn.setColumnType(oldType);
        oldColumn.setMaximumSize(null);

        ColumnModel newColumn = new ColumnModel();
        newColumn.setName("my-column");
        newColumn.setColumnType(newType);
        newColumn.setMaximumSize(newMaxLength);

        assertEquals(SynapseHelper.isCompatibleColumn(oldColumn, newColumn), expected);
    }

    // branch coverage
    @Test(expectedExceptions = BridgeSynapseNonRetryableException.class, expectedExceptionsMessageRegExp =
            "old column my-string has type STRING and no max length")
    public void isCompatibleOldStringWithNoMaxLength() throws Exception {
        ColumnModel oldColumn = new ColumnModel();
        oldColumn.setName("my-string");
        oldColumn.setColumnType(ColumnType.STRING);
        oldColumn.setMaximumSize(null);

        ColumnModel newColumn = new ColumnModel();
        newColumn.setName("my-string");
        newColumn.setColumnType(ColumnType.STRING);
        newColumn.setMaximumSize(42L);

        SynapseHelper.isCompatibleColumn(oldColumn, newColumn);
    }

    // branch coverage
    @Test(expectedExceptions = BridgeSynapseNonRetryableException.class, expectedExceptionsMessageRegExp =
            "new column my-string has type STRING and no max length")
    public void isCompatibleNewStringWithNoMaxLength() throws Exception {
        ColumnModel oldColumn = new ColumnModel();
        oldColumn.setName("my-string");
        oldColumn.setColumnType(ColumnType.STRING);
        oldColumn.setMaximumSize(42L);

        ColumnModel newColumn = new ColumnModel();
        newColumn.setName("my-string");
        newColumn.setColumnType(ColumnType.STRING);
        newColumn.setMaximumSize(null);

        SynapseHelper.isCompatibleColumn(oldColumn, newColumn);
    }

    @Test
    public void isCompatibleColumnNameChange() throws Exception {
        ColumnModel fooColumn = new ColumnModel();
        fooColumn.setName("foo");
        fooColumn.setColumnType(ColumnType.INTEGER);

        ColumnModel barColumn = new ColumnModel();
        barColumn.setName("bar");
        barColumn.setColumnType(ColumnType.INTEGER);

        assertFalse(SynapseHelper.isCompatibleColumn(fooColumn, barColumn));
    }

    // Most of these are retry wrappers, but we should test them anyway for branch coverage.

    @Test
    public void createAclWithAdminAndReadOnly_NewAcl() throws Exception {
        // Mock Synapse Client.
        when(mockSynapseClient.getACL(any())).thenThrow(SynapseNotFoundException.class);

        AccessControlList createdAcl = new AccessControlList();
        when(mockSynapseClient.createACL(any())).thenReturn(createdAcl);

        // Execute and validate.
        AccessControlList retVal = synapseHelper.createAclWithRetry(SYNAPSE_ENTITY_ID, ImmutableSet.of(1111L, 2222L),
                ImmutableSet.of(3333L, 4444L));
        assertSame(retVal, createdAcl);

        ArgumentCaptor<AccessControlList> aclToCreateCaptor = ArgumentCaptor.forClass(AccessControlList.class);
        verify(mockSynapseClient).createACL(aclToCreateCaptor.capture());

        AccessControlList aclToCreate = aclToCreateCaptor.getValue();
        assertEquals(aclToCreate.getId(), SYNAPSE_ENTITY_ID);

        Map<Long, ResourceAccess> principalToAccess = Maps.uniqueIndex(aclToCreate.getResourceAccess(),
                ResourceAccess::getPrincipalId);
        assertEquals(principalToAccess.size(), 4);
        assertEquals(principalToAccess.get(1111L).getAccessType(), ModelConstants.ENTITY_ADMIN_ACCESS_PERMISSIONS);
        assertEquals(principalToAccess.get(2222L).getAccessType(), ModelConstants.ENTITY_ADMIN_ACCESS_PERMISSIONS);
        assertEquals(principalToAccess.get(3333L).getAccessType(), SynapseHelper.ACCESS_TYPE_READ);
        assertEquals(principalToAccess.get(4444L).getAccessType(), SynapseHelper.ACCESS_TYPE_READ);
    }

    @Test
    public void createAclWithAdminAndReadOnly_ExistingAcl() throws Exception {
        // Mock Synapse Client.
        AccessControlList existingAcl = new AccessControlList();
        existingAcl.setEtag(ETAG);
        when(mockSynapseClient.getACL(any())).thenReturn(existingAcl);

        AccessControlList updatedAcl = new AccessControlList();
        when(mockSynapseClient.updateACL(any())).thenReturn(updatedAcl);

        // Execute and validate.
        AccessControlList retVal = synapseHelper.createAclWithRetry(SYNAPSE_ENTITY_ID, ImmutableSet.of(1111L, 2222L),
                ImmutableSet.of(3333L, 4444L));
        assertSame(retVal, updatedAcl);

        // We verify the ACL entries in the previous test. For this one, just verify the etag.
        ArgumentCaptor<AccessControlList> aclToUpdateCaptor = ArgumentCaptor.forClass(AccessControlList.class);
        verify(mockSynapseClient).updateACL(aclToUpdateCaptor.capture());

        AccessControlList aclTUpdate = aclToUpdateCaptor.getValue();
        assertEquals(aclTUpdate.getEtag(), ETAG);
    }

    @Test
    public void createAcl() throws Exception {
        // mock Synapse Client - Unclear whether Synapse client just passes back the input ACL or if it creates a new
        // one. Regardless, don't depend on this implementation. Just return a separate one for tests.
        AccessControlList inputAcl = new AccessControlList();
        AccessControlList outputAcl = new AccessControlList();
        when(mockSynapseClient.createACL(inputAcl)).thenReturn(outputAcl);

        // execute and validate
        AccessControlList retVal = synapseHelper.createAclWithRetry(inputAcl);
        assertSame(retVal, outputAcl);
    }

    @Test
    public void getAcl() throws Exception {
        // Mock Synapse Client.
        AccessControlList acl = new AccessControlList();
        when(mockSynapseClient.getACL(SYNAPSE_ENTITY_ID)).thenReturn(acl);

        // Execute and validate.
        AccessControlList retVal = synapseHelper.getAclWithRetry(SYNAPSE_ENTITY_ID);
        assertSame(retVal, acl);
    }

    @Test
    public void getAcl_NotFound() throws Exception {
        // Mock Synapse Client.
        when(mockSynapseClient.getACL(SYNAPSE_ENTITY_ID)).thenThrow(SynapseNotFoundException.class);

        // Execute and validate.
        AccessControlList retVal = synapseHelper.getAclWithRetry(SYNAPSE_ENTITY_ID);
        assertNull(retVal);
    }

    @Test
    public void updateAcl() throws Exception {
        // mock Synapse Client - Unclear whether Synapse client just passes back the input ACL or if it creates a new
        // one. Regardless, don't depend on this implementation. Just return a separate one for tests.
        AccessControlList inputAcl = new AccessControlList();
        AccessControlList outputAcl = new AccessControlList();
        when(mockSynapseClient.updateACL(inputAcl)).thenReturn(outputAcl);

        // execute and validate
        AccessControlList retVal = synapseHelper.updateAclWithRetry(inputAcl);
        assertSame(retVal, outputAcl);
    }

    @Test
    public void addAnnotationsToEntity() throws Exception {
        // Create test Annotation maps.
        AnnotationsValue existingValue = makeAnnotationsValue("existing value");
        AnnotationsValue oldOverwriteValue = makeAnnotationsValue("old overwrite value");
        AnnotationsValue newOverwriteValue = makeAnnotationsValue("new overwrite value");
        AnnotationsValue addedValue = makeAnnotationsValue("added value");

        Map<String, AnnotationsValue> existingAnnotationMap = new HashMap<>();
        existingAnnotationMap.put("existing", existingValue);
        existingAnnotationMap.put("overwrite", oldOverwriteValue);

        Annotations existingAnnotations = new Annotations();
        existingAnnotations.setEtag(ETAG);
        existingAnnotations.setId(SYNAPSE_ENTITY_ID);
        existingAnnotations.setAnnotations(existingAnnotationMap);

        Map<String, AnnotationsValue> addedAnnotationMap = new HashMap<>();
        addedAnnotationMap.put("overwrite", newOverwriteValue);
        addedAnnotationMap.put("added", addedValue);

        // Mock Synapse Client.
        when(mockSynapseClient.getAnnotationsV2(SYNAPSE_ENTITY_ID)).thenReturn(existingAnnotations);

        Annotations updatedAnnotations = new Annotations();
        when(mockSynapseClient.updateAnnotationsV2(any(), any())).thenReturn(updatedAnnotations);

        // Execute and validate.
        Annotations retVal = synapseHelper.addAnnotationsToEntity(SYNAPSE_ENTITY_ID, addedAnnotationMap);
        assertSame(retVal, updatedAnnotations);

        ArgumentCaptor<Annotations> annotationsToUpdateCaptor = ArgumentCaptor.forClass(Annotations.class);
        verify(mockSynapseClient).updateAnnotationsV2(eq(SYNAPSE_ENTITY_ID), annotationsToUpdateCaptor.capture());
        Annotations annotationsToUpdate = annotationsToUpdateCaptor.getValue();
        assertEquals(annotationsToUpdate.getEtag(), ETAG);
        assertEquals(annotationsToUpdate.getId(), SYNAPSE_ENTITY_ID);

        Map<String, AnnotationsValue> annotationsToUpdateMap = annotationsToUpdate.getAnnotations();
        assertEquals(annotationsToUpdateMap.size(), 3);
        assertEquals(annotationsToUpdateMap.get("existing"), existingValue);
        assertEquals(annotationsToUpdateMap.get("overwrite"), newOverwriteValue);
        assertEquals(annotationsToUpdateMap.get("added"), addedValue);
    }

    private static AnnotationsValue makeAnnotationsValue(String value) {
        AnnotationsValue annotationsValue = new AnnotationsValue();
        annotationsValue.setType(AnnotationsValueType.STRING);
        annotationsValue.setValue(ImmutableList.of(value));
        return annotationsValue;
    }

    @Test
    public void getAnnotations() throws Exception {
        // Mock Synapse Client.
        Annotations annotations = new Annotations();
        when(mockSynapseClient.getAnnotationsV2(SYNAPSE_ENTITY_ID)).thenReturn(annotations);

        // Execute and validate.
        Annotations retVal = synapseHelper.getAnnotationsWithRetry(SYNAPSE_ENTITY_ID);
        assertSame(retVal, annotations);
    }

    @Test
    public void updateAnnotations() throws Exception {
        //Mock Synapse Client.
        Annotations createdAnnotations = new Annotations();
        when(mockSynapseClient.updateAnnotationsV2(any(), any())).thenReturn(createdAnnotations);

        // Execute and validate.
        Annotations annotationsToCreate = new Annotations();
        Annotations retVal = synapseHelper.updateAnnotationsWithRetry(SYNAPSE_ENTITY_ID, annotationsToCreate);
        assertSame(retVal, createdAnnotations);

        verify(mockSynapseClient).updateAnnotationsV2(eq(SYNAPSE_ENTITY_ID), same(annotationsToCreate));
    }

    @Test
    public void lookupChild() throws Exception {
        // Mock Synapse Client.
        when(mockSynapseClient.lookupChild(any(), any())).thenReturn(SYNAPSE_CHILD_ID);

        // Execute and validate.
        String childId = synapseHelper.lookupChildWithRetry(SYNAPSE_PARENT_ID, ENTITY_NAME_CHILD);
        assertEquals(childId, SYNAPSE_CHILD_ID);

        verify(mockSynapseClient).lookupChild(SYNAPSE_PARENT_ID, ENTITY_NAME_CHILD);
    }

    @Test
    public void lookupChildNotFound() throws Exception {
        // Mock Synapse Client.
        when(mockSynapseClient.lookupChild(any(), any())).thenThrow(new SynapseNotFoundException());

        // Execute and validate.
        String childId = synapseHelper.lookupChildWithRetry(SYNAPSE_PARENT_ID, ENTITY_NAME_CHILD);
        assertNull(childId);
    }

    @Test
    public void createColumnModels() throws Exception {
        // mock Synapse Client - Similarly, return a new output list
        List<ColumnModel> inputColumnModelList = ImmutableList.of(new ColumnModel());
        List<ColumnModel> outputColumnModelList = ImmutableList.of(new ColumnModel());
        when(mockSynapseClient.createColumnModels(inputColumnModelList)).thenReturn(outputColumnModelList);

        // execute and validate
        List<ColumnModel> retVal = synapseHelper.createColumnModelsWithRetry(inputColumnModelList);
        assertSame(retVal, outputColumnModelList);
    }

    @Test
    public void createEntity() throws Exception {
        // mock Synapse Client - Similarly, return a new output table
        TableEntity inputTable = new TableEntity();
        TableEntity outputTable = new TableEntity();
        when(mockSynapseClient.createEntity(inputTable)).thenReturn(outputTable);

        // execute and validate
        TableEntity retVal = synapseHelper.createEntityWithRetry(inputTable);
        assertSame(retVal, outputTable);
    }

    @Test
    public void getEntity() throws Exception {
        // Mock Synapse Client.
        TableEntity outputTable = new TableEntity();
        when(mockSynapseClient.getEntity(SYNAPSE_ENTITY_ID, TableEntity.class)).thenReturn(outputTable);

        // Execute and validate.
        TableEntity retVal = synapseHelper.getEntityWithRetry(SYNAPSE_ENTITY_ID, TableEntity.class);
        assertSame(retVal, outputTable);
    }

    @Test
    public void getEntity_NotFound() throws Exception {
        // Mock Synapse Client.
        when(mockSynapseClient.getEntity(SYNAPSE_ENTITY_ID, TableEntity.class))
                .thenThrow(SynapseNotFoundException.class);

        // Execute and validate.
        TableEntity retVal = synapseHelper.getEntityWithRetry(SYNAPSE_ENTITY_ID, TableEntity.class);
        assertNull(retVal);
    }

    @Test
    public void updateEntity() throws Exception {
        // mock Synapse Client - Similarly, return a new output table
        TableEntity inputTable = new TableEntity();
        TableEntity outputTable = new TableEntity();
        when(mockSynapseClient.putEntity(inputTable)).thenReturn(outputTable);

        // execute and validate
        TableEntity retVal = synapseHelper.updateEntityWithRetry(inputTable);
        assertSame(retVal, outputTable);
    }

    @Test
    public void createFileHandle() throws Exception {
        // mock Synapse Client
        File mockFile = mock(File.class);
        S3FileHandle mockFileHandle = mock(S3FileHandle.class);
        when(mockSynapseClient.multipartUpload(mockFile, null, null, null)).thenReturn(mockFileHandle);

        // execute and validate
        FileHandle retVal = synapseHelper.createFileHandleWithRetry(mockFile);
        assertSame(retVal, mockFileHandle);
    }

    @Test
    public void createS3FileHandle() throws Exception {
        // Mock Synapse Client.
        S3FileHandle createdFileHandle = new S3FileHandle();
        when(mockSynapseClient.createExternalS3FileHandle(any())).thenReturn(createdFileHandle);

        // Execute and validate.
        S3FileHandle inputFileHandle = new S3FileHandle();
        S3FileHandle outputFileHandle = synapseHelper.createS3FileHandleWithRetry(inputFileHandle);
        assertSame(outputFileHandle, createdFileHandle);

        verify(mockSynapseClient).createExternalS3FileHandle(inputFileHandle);
    }

    @Test
    public void createFolderAlreadyExists() throws Exception {
        // Mock Synapse Client.
        when(mockSynapseClient.lookupChild(SYNAPSE_PARENT_ID, ENTITY_NAME_CHILD)).thenReturn(SYNAPSE_CHILD_ID);

        // Execute and validate.
        String folderId = synapseHelper.createFolderIfNotExists(SYNAPSE_PARENT_ID, ENTITY_NAME_CHILD);
        assertEquals(folderId, SYNAPSE_CHILD_ID);

        verify(mockSynapseClient, never()).createEntity(any());
    }

    @Test
    public void createFolderDoesNotExist() throws Exception {
        // Mock Synapse Client.
        when(mockSynapseClient.lookupChild(SYNAPSE_PARENT_ID, ENTITY_NAME_CHILD)).thenReturn(null);

        Folder folder = new Folder();
        folder.setId(SYNAPSE_CHILD_ID);
        when(mockSynapseClient.createEntity(any(Folder.class))).thenReturn(folder);

        // Execute and validate.
        String folderId = synapseHelper.createFolderIfNotExists(SYNAPSE_PARENT_ID, ENTITY_NAME_CHILD);
        assertEquals(folderId, SYNAPSE_CHILD_ID);

        ArgumentCaptor<Folder> createdFolderCaptor = ArgumentCaptor.forClass(Folder.class);
        verify(mockSynapseClient).createEntity(createdFolderCaptor.capture());
        Folder createdFolder = createdFolderCaptor.getValue();
        assertEquals(createdFolder.getParentId(), SYNAPSE_PARENT_ID);
        assertEquals(createdFolder.getName(), ENTITY_NAME_CHILD);
    }

    @DataProvider(name = "isSynapseWritableProvider")
    public Object[][] isSynapseWritableProvider() {
        // { status, expected }
        return new Object[][] {
                { StatusEnum.READ_WRITE, true },
                { StatusEnum.READ_ONLY, false },
                { StatusEnum.DOWN, false },
        };
    }

    @Test(dataProvider = "isSynapseWritableProvider")
    public void isSynapseWritable(StatusEnum status, boolean expected) throws Exception {
        // Mock synapse client.
        StackStatus stackStatus = new StackStatus();
        stackStatus.setStatus(status);
        when(mockSynapseClient.getCurrentStackStatus()).thenReturn(stackStatus);

        // Execute and validate.
        boolean retVal = synapseHelper.isSynapseWritable();
        assertEquals(retVal, expected);
    }

    @Test
    public void createProjectSetting() throws Exception {
        // Mock Synapse Client.
        UploadDestinationListSetting createdProjectSetting = new UploadDestinationListSetting();
        when(mockSynapseClient.createProjectSetting(any())).thenReturn(createdProjectSetting);

        // Execute and validate.
        UploadDestinationListSetting projectSettingToCreate = new UploadDestinationListSetting();
        ProjectSetting retVal = synapseHelper.createProjectSettingWithRetry(projectSettingToCreate);
        assertSame(retVal, createdProjectSetting);

        verify(mockSynapseClient).createProjectSetting(same(projectSettingToCreate));
    }

    @Test
    public void createStorageLocation() throws Exception {
        // Mock Synapse Client.
        ExternalS3StorageLocationSetting createdStorageLocation = new ExternalS3StorageLocationSetting();
        when(mockSynapseClient.createStorageLocationSetting(any())).thenReturn(createdStorageLocation);

        // Execute and validate.
        ExternalS3StorageLocationSetting storageLocationToCreate = new ExternalS3StorageLocationSetting();
        ExternalS3StorageLocationSetting retVal = synapseHelper.createStorageLocationWithRetry(
                storageLocationToCreate);
        assertSame(retVal, createdStorageLocation);

        verify(mockSynapseClient).createStorageLocationSetting(same(storageLocationToCreate));
    }

    @Test
    public void createStorageLocationForEntity() throws Exception {
        // Mock Synapse Client.
        ExternalS3StorageLocationSetting createdStorageLocation = new ExternalS3StorageLocationSetting();
        createdStorageLocation.setStorageLocationId(STORAGE_LOCATION_ID);
        when(mockSynapseClient.createStorageLocationSetting(any())).thenReturn(createdStorageLocation);

        // Execute and validate.
        ExternalS3StorageLocationSetting storageLocationToCreate = new ExternalS3StorageLocationSetting();
        ExternalS3StorageLocationSetting retVal = synapseHelper.createStorageLocationForEntity(SYNAPSE_ENTITY_ID,
                storageLocationToCreate);
        assertSame(retVal, createdStorageLocation);

        verify(mockSynapseClient).createStorageLocationSetting(same(storageLocationToCreate));

        ArgumentCaptor<ProjectSetting> projectSettingToCreateCaptor = ArgumentCaptor.forClass(ProjectSetting.class);
        verify(mockSynapseClient).createProjectSetting(projectSettingToCreateCaptor.capture());
        UploadDestinationListSetting projectSettingToCreate = (UploadDestinationListSetting)
                projectSettingToCreateCaptor.getValue();
        assertEquals(projectSettingToCreate.getLocations(), ImmutableList.of(STORAGE_LOCATION_ID));
        assertEquals(projectSettingToCreate.getProjectId(), SYNAPSE_ENTITY_ID);
        assertEquals(projectSettingToCreate.getSettingsType(), ProjectSettingsType.upload);
    }

    @Test
    public void createTable() throws Exception {
        // mock Synapse Client - Similarly, return a new output table
        TableEntity inputTable = new TableEntity();
        TableEntity outputTable = new TableEntity();
        when(mockSynapseClient.createEntity(inputTable)).thenReturn(outputTable);

        // execute and validate
        TableEntity retVal = synapseHelper.createTableWithRetry(inputTable);
        assertSame(retVal, outputTable);
    }

    @Test
    public void getColumnModelsForTable() throws Exception {
        // mock Synapse Client
        List<ColumnModel> outputColumnModelList = ImmutableList.of(new ColumnModel());
        when(mockSynapseClient.getColumnModelsForTableEntity("table-id")).thenReturn(outputColumnModelList);

        // execute and validate
        List<ColumnModel> retVal = synapseHelper.getColumnModelsForTableWithRetry("table-id");
        assertSame(retVal, outputColumnModelList);
    }

    @Test
    public void getTable() throws Exception {
        // mock Synapse Client
        TableEntity tableEntity = new TableEntity();
        when(mockSynapseClient.getEntity("table-id", TableEntity.class)).thenReturn(tableEntity);

        // execute and validate
        TableEntity retVal = synapseHelper.getTableWithRetry("table-id");
        assertSame(retVal, tableEntity);
    }

    @Test
    public void startTableTransaction() throws Exception {
        // mock Synapse Client
        List<TableUpdateRequest> dummyChangeList = ImmutableList.of();
        when(mockSynapseClient.startTableTransactionJob(same(dummyChangeList), eq("table-id"))).thenReturn(
                "job-token");

        // execute and validate
        String retVal = synapseHelper.startTableTransactionWithRetry(dummyChangeList, "table-id");
        assertEquals(retVal, "job-token");
    }

    @Test
    public void getTableTransactionResult() throws Exception {
        // mock Synapse Client
        List<TableUpdateResponse> dummyResponseList = ImmutableList.of();
        when(mockSynapseClient.getTableTransactionJobResults("job-token", "table-id")).thenReturn(dummyResponseList);

        // execute and validate
        List<TableUpdateResponse> retVal = synapseHelper.getTableTransactionResultWithRetry("job-token", "table-id");
        assertSame(retVal, dummyResponseList);
    }

    @Test
    public void getTableTransactionResultNotReady() throws Exception {
        // mock Synapse Client
        when(mockSynapseClient.getTableTransactionJobResults("job-token", "table-id")).thenThrow(
                SynapseResultNotReadyException.class);

        // execute and validate
        List<TableUpdateResponse> retVal = synapseHelper.getTableTransactionResultWithRetry("job-token", "table-id");
        assertNull(retVal);
    }

    @Test
    public void uploadTsvStart() throws Exception {
        // mock Synapse Client
        CsvTableDescriptor inputTableDesc = new CsvTableDescriptor();
        when(mockSynapseClient.uploadCsvToTableAsyncStart("table-id", "file-handle-id", null, null, inputTableDesc,
                null)).thenReturn("job-token");

        // execute and validate
        String retVal = synapseHelper.uploadTsvStartWithRetry("table-id", "file-handle-id", inputTableDesc);
        assertEquals(retVal, "job-token");
    }

    @Test
    public void uploadTsvStatus() throws Exception {
        // mock SynapseClient
        UploadToTableResult result = new UploadToTableResult();
        when(mockSynapseClient.uploadCsvToTableAsyncGet("job-token", "table-id")).thenReturn(result);

        // execute and validate
        UploadToTableResult retVal = synapseHelper.getUploadTsvStatus("job-token", "table-id");
        assertSame(retVal, result);
    }

    @Test
    public void uploadTsvStatusNotReady() throws Exception {
        // mock SynapseClient
        when(mockSynapseClient.uploadCsvToTableAsyncGet("job-token", "table-id"))
                .thenThrow(SynapseResultNotReadyException.class);

        // execute and validate
        UploadToTableResult retVal = synapseHelper.getUploadTsvStatus("job-token", "table-id");
        assertNull(retVal);
    }

    @Test
    public void createTeam() throws Exception {
        // Mock SynapseClient.
        Team createdTeam = new Team();
        when(mockSynapseClient.createTeam(any())).thenReturn(createdTeam);

        // Execute and validate.
        Team teamToCreate = new Team();
        Team retVal = synapseHelper.createTeamWithRetry(teamToCreate);
        assertSame(retVal, createdTeam);
        verify(mockSynapseClient).createTeam(same(teamToCreate));
    }
}
