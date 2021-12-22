package org.sagebionetworks.bridge.synapse;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.amazonaws.AmazonClientException;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.RateLimiter;
import com.jcabi.aspects.RetryOnFailure;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.client.exceptions.SynapseNotFoundException;
import org.sagebionetworks.client.exceptions.SynapseResultNotReadyException;
import org.sagebionetworks.repo.model.ACCESS_TYPE;
import org.sagebionetworks.repo.model.AccessControlList;
import org.sagebionetworks.repo.model.Entity;
import org.sagebionetworks.repo.model.Folder;
import org.sagebionetworks.repo.model.ResourceAccess;
import org.sagebionetworks.repo.model.Team;
import org.sagebionetworks.repo.model.annotation.v2.Annotations;
import org.sagebionetworks.repo.model.annotation.v2.AnnotationsValue;
import org.sagebionetworks.repo.model.file.FileHandle;
import org.sagebionetworks.repo.model.file.S3FileHandle;
import org.sagebionetworks.repo.model.project.ProjectSetting;
import org.sagebionetworks.repo.model.project.ProjectSettingsType;
import org.sagebionetworks.repo.model.project.StorageLocationSetting;
import org.sagebionetworks.repo.model.project.UploadDestinationListSetting;
import org.sagebionetworks.repo.model.status.StackStatus;
import org.sagebionetworks.repo.model.status.StatusEnum;
import org.sagebionetworks.repo.model.table.AppendableRowSet;
import org.sagebionetworks.repo.model.table.ColumnChange;
import org.sagebionetworks.repo.model.table.ColumnModel;
import org.sagebionetworks.repo.model.table.ColumnType;
import org.sagebionetworks.repo.model.table.CsvTableDescriptor;
import org.sagebionetworks.repo.model.table.RowReferenceSet;
import org.sagebionetworks.repo.model.table.TableEntity;
import org.sagebionetworks.repo.model.table.TableSchemaChangeRequest;
import org.sagebionetworks.repo.model.table.TableSchemaChangeResponse;
import org.sagebionetworks.repo.model.table.TableUpdateRequest;
import org.sagebionetworks.repo.model.table.TableUpdateResponse;
import org.sagebionetworks.repo.model.table.UploadToTableResult;
import org.sagebionetworks.repo.model.util.ModelConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.sagebionetworks.bridge.exceptions.BridgeSynapseException;
import org.sagebionetworks.bridge.exceptions.BridgeSynapseNonRetryableException;

/** Synapse operations that are common across multiple workers and should be shared. */
public class SynapseHelper {
    private static final Logger LOG = LoggerFactory.getLogger(SynapseHelper.class);

    private static final Joiner COMMA_SPACE_JOINER = Joiner.on(", ").useForNull("");

    // Package-scoped for unit tests.
    static final Set<ACCESS_TYPE> ACCESS_TYPE_READ = ImmutableSet.of(ACCESS_TYPE.READ, ACCESS_TYPE.DOWNLOAD);

    // Map of allowed column type changes. Key is the old type. Value is the new type.
    //
    // A few notes: This is largely based on whether the data can be converted in Synapse tables. Since booleans are
    // stored as 0/1 and dates are stored as epoch milliseconds, converting these to strings means old values will be
    // numeric types, but new values are likely to be "true"/"false" or ISO8601 timestamps. This leads to more
    // confusion overall, so we've decided to block it.
    //
    // Similarly, if you convert a bool to a numeric type (int, float), Synapse will convert the bools to 0s and 1s.
    // However, old bools in DynamoDB are still using "true"/"false", which will no longer serialize to Synapse. To
    // prevent this data loss, we're also not allowing bools to convert to numeric types.
    //
    // Also note that due to a bug, we cannot currently convert anything to a LargeText.
    // See https://sagebionetworks.jira.com/browse/PLFM-4028
    private static final SetMultimap<ColumnType, ColumnType> ALLOWED_OLD_TYPE_TO_NEW_TYPE =
            ImmutableSetMultimap.<ColumnType, ColumnType>builder()
                    // Numeric types can changed to types with more precision (int to float), but not less
                    // precision (float to int).
                    .put(ColumnType.INTEGER, ColumnType.DOUBLE)
                    // Date can be converted to int and float (epoch milliseconds), and can be converted back from an
                    // int. However, we block converting from float, since that causes data loss.
                    .put(ColumnType.DATE, ColumnType.INTEGER)
                    .put(ColumnType.DATE, ColumnType.DOUBLE)
                    .put(ColumnType.INTEGER, ColumnType.DATE)
                    // Numeric types are trivially converted to strings.
                    .put(ColumnType.DOUBLE, ColumnType.STRING)
                    .put(ColumnType.INTEGER, ColumnType.STRING)
                    .build();

    /**
     * The max lengths of various Synapse column types. This is used to determine how long a Synapse column can be
     * when we convert the column to a String. This only contains column types that can be converted to Strings. Note
     * that it excludes dates and bools, as Synapse considers these numeric types, but Bridge uses ISO8601 and
     * "true"/"false".
     */
    private static final Map<ColumnType, Integer> SYNAPSE_TYPE_TO_MAX_LENGTH =
            ImmutableMap.<ColumnType, Integer>builder()
                    // Empirically, the longest float in Synapse is 22 chars long
                    .put(ColumnType.DOUBLE, 22)
                    // Synapse uses a bigint (signed long), which can be 20 chars long
                    .put(ColumnType.INTEGER, 20)
                    .build();

    private int asyncIntervalMillis = 1000;
    private int asyncTimeoutLoops = 300;
    private SynapseClient synapseClient;

    // Rate limiter, used to limit the amount of traffic to Synapse. Synapse throttles at 10 requests per second.
    private final RateLimiter rateLimiter = RateLimiter.create(10.0);

    // Rate limiter for getColumnModelsForEntity(). Empirically, Synapse throttles at 24 requests per minute. Add a
    // safety factor and rate limit to 12 per minute.
    private final RateLimiter getColumnModelsRateLimiter = RateLimiter.create(12.0 / 60.0);

    /** Set the number of milliseconds between loops while polling async requests in Synapse. Defaults to 1000. */
    public final void setAsyncIntervalMillis(int asyncIntervalMillis) {
        this.asyncIntervalMillis = asyncIntervalMillis;
    }

    /**
     * Sets the number of loops while polling async requests in Synapse before we time out the request. Default is 300
     * (5 minutes, using the default async interval millis).
     */
    public final void setAsyncTimeoutLoops(int asyncTimeoutLoops) {
        this.asyncTimeoutLoops = asyncTimeoutLoops;
    }

    /**
     * Set the rate limiting for general traffic to Synapse. Generally used for testing or to tweak rate limits
     * without a code change. Values are per second. Default is 10.0.
     */
    public final void setRateLimit(double rateLimit) {
        rateLimiter.setRate(rateLimit);
    }

    /**
     * Set the rate limiting for getColumnModels. Generally used for testing or to tweak rate limits without a code
     * change. Values are per second. Default is 0.2 (12 per minute).
     */
    public final void setGetColumnModelsRateLimit(double rateLimit) {
        getColumnModelsRateLimiter.setRate(rateLimit);
    }

    /** Synapse client. */
    public final void setSynapseClient(SynapseClient synapseClient) {
        this.synapseClient = synapseClient;
    }

    /**
     * Helper method to create a table with the specified columns and set up ACLs. The read-only principal IDs are set
     * with read and download permissions, while the admin principal IDs are set with admin permissions.
     *
     * @param columnList
     *         list of column models to create on the table
     * @param readOnlyPrincipalIdSet
     *         principal IDs (users or teams) that should have read permissions
     * @param adminPrincipalIdSet
     *         principal IDs (users or teams), that should have admin permissions
     * @param projectId
     *         Synapse project to create the table in
     * @param tableName
     *         table name
     * @return Synapse table ID
     * @throws BridgeSynapseException
     *         under unexpected circumstances, like a table created with the wrong number of columns
     * @throws SynapseException
     *         if the underlying Synapse calls fail
     */
    public String createTableWithColumnsAndAcls(List<ColumnModel> columnList, Set<Long> readOnlyPrincipalIdSet,
            Set<Long> adminPrincipalIdSet, String projectId, String tableName) throws BridgeSynapseException,
            SynapseException {
        // Create columns
        List<ColumnModel> createdColumnList = createColumnModelsWithRetry(columnList);
        if (columnList.size() != createdColumnList.size()) {
            throw new BridgeSynapseException("Error creating Synapse table " + tableName + ": Tried to create " +
                    columnList.size() + " columns. Actual: " + createdColumnList.size() + " columns.");
        }

        List<String> columnIdList = new ArrayList<>();
        //noinspection Convert2streamapi
        for (ColumnModel oneCreatedColumn : createdColumnList) {
            columnIdList.add(oneCreatedColumn.getId());
        }

        // create table
        TableEntity synapseTable = new TableEntity();
        synapseTable.setName(tableName);
        synapseTable.setParentId(projectId);
        synapseTable.setColumnIds(columnIdList);
        TableEntity createdTable = createTableWithRetry(synapseTable);
        String synapseTableId = createdTable.getId();

        // create ACLs
        createAclWithRetry(synapseTableId, adminPrincipalIdSet, readOnlyPrincipalIdSet);

        return synapseTableId;
    }

    /**
     * Returns true if the old column can be converted to the new column in a meaningful way without data loss. Used to
     * determine if the schema changes, whether Bridge should try to modify the table.
     *
     * @param oldColumn
     *         column model currently in Synapse
     * @param newColumn
     *         column model we want to replace it with
     * @return true if they're compatible, false otherwise
     * @throws BridgeSynapseException
     *         if there's an unexpected error comparing the columns
     */
    public static boolean isCompatibleColumn(ColumnModel oldColumn, ColumnModel newColumn)
            throws BridgeSynapseException {
        // Ignore the following attributes:
        // ID - The ones from Synapse will have IDs, but the ones we generate from the schema won't. This is normal.
        // defaultValues, enumValues - We don't use these, and changing these won't affect data integrity.

        // If types are different, check the table.
        if (oldColumn.getColumnType() != newColumn.getColumnType()) {
            Set<ColumnType> allowedNewTypes = ALLOWED_OLD_TYPE_TO_NEW_TYPE.get(oldColumn.getColumnType());
            if (!allowedNewTypes.contains(newColumn.getColumnType())) {
                return false;
            }
        }

        // For string types, check max length. You can increase the max length, but you can't decrease it.
        if (newColumn.getColumnType() == ColumnType.STRING &&
                !Objects.equals(oldColumn.getMaximumSize(), newColumn.getMaximumSize())) {
            long oldMaxLength;
            if (oldColumn.getMaximumSize() != null) {
                // If the old field def specified a max length, just use it.
                oldMaxLength = oldColumn.getMaximumSize();
            } else if (SYNAPSE_TYPE_TO_MAX_LENGTH.containsKey(oldColumn.getColumnType())) {
                // The max length of the old field type is specified by its type.
                oldMaxLength = SYNAPSE_TYPE_TO_MAX_LENGTH.get(oldColumn.getColumnType());
            } else {
                // This should never happen. If we get here, that means we somehow have a String column in Synapse
                // without a Max Length parameter. Abort and throw.
                throw new BridgeSynapseNonRetryableException("old column " + oldColumn.getName() + " has type " +
                        oldColumn.getColumnType() + " and no max length");
            }

            if (newColumn.getMaximumSize() == null) {
                // This should also never happen. This means that we generated a String column without a Max Length,
                // which is bad.
                throw new BridgeSynapseNonRetryableException("new column " + newColumn.getName() + " has type " +
                        "STRING and no max length");
            }
            long newMaxLength = newColumn.getMaximumSize();

            // You can't decrease max length.
            if (newMaxLength < oldMaxLength) {
                return false;
            }
        }

        // This should never happen, but if the names are somehow different, they aren't compatible.
        if (!Objects.equals(oldColumn.getName(), newColumn.getName())) {
            return false;
        }

        // If we passed all incompatibility checks, then we're compatible.
        return true;
    }

    /**
     * <p>
     * Helper method to detect when a schema changes and updates the Synapse table accordingly. Will reject schema
     * changes that delete or modify columns. Optimized so if no columns were inserted, it won't modify the table.
     * </p>
     * <p>
     * The mergeDeletedFields flag controls how this method handles deleted fields. If a field exists in the table, but
     * is missing from the newColumnList, that field is flagged as a "deleted field". If mergeDeletedFields is set to
     * true, this method will automatically append those fields to the newColumnList, thereby preserving those fields
     * in the table. If mergeDeletedFields is set to false, this method will throw an error. Note that modified fields
     * will throw an error no matter what.
     * </p>
     *
     * @param synapseTableId
     *         table to update
     * @param newColumnList
     *         columns to update the table with
     * @param mergeDeletedFields
     *         true to merge deleted fields, false to throw an error
     * @throws BridgeSynapseException
     *         if the column changes are incompatible
     * @throws SynapseException
     *         if the underlying Synapse calls fail
     */
    public void safeUpdateTable(String synapseTableId, List<ColumnModel> newColumnList, boolean mergeDeletedFields)
            throws BridgeSynapseException, SynapseException {
        // Copy newColumnList, in case it's immutable or otherwise has reason to not be modified.
        newColumnList = new ArrayList<>(newColumnList);

        // Get existing columns from table.
        List<ColumnModel> existingColumnList = getColumnModelsForTableWithRetry(synapseTableId);

        // Compute the columns that were added, deleted, and kept.
        Map<String, ColumnModel> existingColumnsByName = Maps.uniqueIndex(existingColumnList, ColumnModel::getName);
        Map<String, ColumnModel> newColumnsByName = Maps.uniqueIndex(newColumnList, ColumnModel::getName);

        Set<String> addedColumnNameSet = Sets.difference(newColumnsByName.keySet(), existingColumnsByName.keySet());
        Set<String> deletedColumnNameSet = Sets.difference(existingColumnsByName.keySet(), newColumnsByName.keySet());
        Set<String> keptColumnNameSet = Sets.intersection(existingColumnsByName.keySet(), newColumnsByName.keySet());

        // Were columns deleted? If so, log an error and shortcut. (Don't modify the table.)
        boolean shouldThrow = false;
        if (!deletedColumnNameSet.isEmpty()) {
            if (mergeDeletedFields) {
                deletedColumnNameSet.stream().map(existingColumnsByName::get).forEach(newColumnList::add);
            } else {
                LOG.error("Table " + synapseTableId + " has deleted columns: " + COMMA_SPACE_JOINER.join(
                        deletedColumnNameSet));
                shouldThrow = true;
            }
        }

        // Similarly, were any columns changed?
        Set<String> modifiedColumnNameSet = new HashSet<>();
        Set<String> incompatibleColumnNameSet = new HashSet<>();
        for (String oneKeptColumnName : keptColumnNameSet) {
            // Was this column modified? Check type and max length, since those are the only fields we care about. We
            // can't use .equals(), because newly generated columns don't have IDs.
            ColumnModel existingColumn = existingColumnsByName.get(oneKeptColumnName);
            ColumnModel newColumn = newColumnsByName.get(oneKeptColumnName);
            if (existingColumn.getColumnType() != newColumn.getColumnType() ||
                    !Objects.equals(existingColumn.getMaximumSize(), newColumn.getMaximumSize())) {
                modifiedColumnNameSet.add(oneKeptColumnName);
            } else {
                continue;
            }

            // This column was modified. Was the modification compatible?
            if (!isCompatibleColumn(existingColumn, newColumn)) {
                incompatibleColumnNameSet.add(oneKeptColumnName);
            }
        }
        if (!incompatibleColumnNameSet.isEmpty()) {
            LOG.error("Table " + synapseTableId + " has incompatible modified columns: " + COMMA_SPACE_JOINER.join(
                    incompatibleColumnNameSet));
            shouldThrow = true;
        }

        if (shouldThrow) {
            throw new BridgeSynapseNonRetryableException("Table has deleted and/or modified columns");
        }

        // Optimization: Were any columns added or modified?
        if (addedColumnNameSet.isEmpty() && modifiedColumnNameSet.isEmpty()) {
            return;
        }

        // Make sure the columns have been created / get column IDs.
        List<ColumnModel> createdColumnList = createColumnModelsWithRetry(newColumnList);

        // Create list of column changes.
        List<ColumnChange> columnChangeList = new ArrayList<>();
        List<String> colIdList = new ArrayList<>();
        for (ColumnModel oneCreatedColumn : createdColumnList) {
            ColumnModel existingColumn = existingColumnsByName.get(oneCreatedColumn.getName());
            String createdColumnId = oneCreatedColumn.getId();
            String existingColumnId = existingColumn != null ? existingColumn.getId() : null;

            // Only add column change if the column is different.
            if (!Objects.equals(createdColumnId, existingColumnId)) {
                ColumnChange columnChange = new ColumnChange();
                columnChange.setOldColumnId(existingColumnId);
                columnChange.setNewColumnId(createdColumnId);
                columnChangeList.add(columnChange);
            }

            // Want to specify order of columns.
            colIdList.add(createdColumnId);
        }

        // Create schema change request.
        TableSchemaChangeRequest schemaChangeRequest = new TableSchemaChangeRequest();
        schemaChangeRequest.setEntityId(synapseTableId);
        schemaChangeRequest.setChanges(columnChangeList);
        schemaChangeRequest.setOrderedColumnIds(colIdList);

        // Update table.
        updateTableColumns(schemaChangeRequest, synapseTableId);
    }

    /**
     * Updates table columns.
     *
     * @param schemaChangeRequest
     *         requested change
     * @param tableId
     *         table to update
     * @return table change response
     * @throws BridgeSynapseException
     *         if there's a general error with Bridge
     * @throws SynapseException
     *         if there's an error calling Synapse
     */
    public TableSchemaChangeResponse updateTableColumns(TableSchemaChangeRequest schemaChangeRequest, String tableId)
            throws BridgeSynapseException, SynapseException {
        // For convenience, this API only contains a single TableSchemaChangeRequest, but the Synapse API takes a whole
        // list. Wrap it in a list.
        List<TableUpdateRequest> changeList = ImmutableList.of(schemaChangeRequest);

        // Start the table update job.
        String jobToken = startTableTransactionWithRetry(changeList, tableId);

        // Poll async get until success or timeout.
        boolean success = false;
        List<TableUpdateResponse> responseList = null;
        for (int loops = 0; loops < asyncTimeoutLoops; loops++) {
            if (asyncIntervalMillis > 0) {
                try {
                    Thread.sleep(asyncIntervalMillis);
                } catch (InterruptedException ex) {
                    // noop
                }
            }

            // poll
            responseList = getTableTransactionResultWithRetry(jobToken, tableId);
            if (responseList != null) {
                success = true;
                break;
            }

            // Result not ready. Loop around again.
        }

        if (!success) {
            throw new BridgeSynapseException("Timed out updating table columns for table " + tableId);
        }

        // The list should have a single response, and it should be a TableSchemaChangeResponse.
        if (responseList.size() != 1) {
            throw new BridgeSynapseException("Expected one table update response for table " + tableId + ", but got " +
                    responseList.size());
        }
        TableUpdateResponse singleResponse = responseList.get(0);
        if (!(singleResponse instanceof TableSchemaChangeResponse)) {
            throw new BridgeSynapseException("Expected a TableSchemaChangeResponse for table " + tableId +
                    ", but got " + singleResponse.getClass().getName());
        }

        return (TableSchemaChangeResponse) singleResponse;
    }

    /**
     * Takes a TSV file from disk and uploads and applies its rows to a Synapse table.
     *
     * @param tableId
     *         Synapse table ID to upload the TSV to
     * @param file
     *         TSV file to apply to the table
     * @return number of rows processed
     * @throws BridgeSynapseException
     *         if there's a general error calling Synapse
     * @throws IOException
     *         if there's an error uploading the file handle
     * @throws SynapseException
     *         if there's an error calling Synapse
     */
    public long uploadTsvFileToTable(String tableId, File file) throws BridgeSynapseException,
            IOException, SynapseException {
        // Upload TSV as a file handle.
        FileHandle tableFileHandle = createFileHandleWithRetry(file);
        String fileHandleId = tableFileHandle.getId();

        // start tsv import
        CsvTableDescriptor tableDesc = new CsvTableDescriptor();
        tableDesc.setIsFirstLineHeader(true);
        tableDesc.setSeparator("\t");
        String jobToken = uploadTsvStartWithRetry(tableId, fileHandleId, tableDesc);

        // poll asyncGet until success or timeout
        boolean success = false;
        Long linesProcessed = null;
        for (int loops = 0; loops < asyncTimeoutLoops; loops++) {
            if (asyncIntervalMillis > 0) {
                try {
                    Thread.sleep(asyncIntervalMillis);
                } catch (InterruptedException ex) {
                    // noop
                }
            }

            // poll
            UploadToTableResult uploadResult = getUploadTsvStatus(jobToken, tableId);
            if (uploadResult != null) {
                linesProcessed = uploadResult.getRowsProcessed();
                success = true;
                break;
            }

            // Result not ready. Loop around again.
        }

        if (!success) {
            throw new BridgeSynapseException("Timed out uploading file handle " + fileHandleId);
        }
        if (linesProcessed == null) {
            // Not sure if Synapse will ever do this, but code defensively, just in case.
            throw new BridgeSynapseException("Null rows processed");
        }

        return linesProcessed;
    }

    /**
     * <p>
     * Convenience method for creating ACLs in Synapse. This method allows you to assign admin and read-only
     * privileges, which is a common use case for Bridge. If you need more fine-grained control, call
     * {@link #createAclWithRetry(AccessControlList)}.
     * </p>
     * <p>
     * If the entity already has ACLs, this method overwrites those ACLs.
     * </p>
     */
    public AccessControlList createAclWithRetry(String entityId, Set<Long> adminPrincipalIdSet,
            Set<Long> readOnlyPrincipalIdSet) throws SynapseException {
        // ResourceAccess is a mutable object, but the Synapse API takes them in a Set. This is a little weird.
        // IMPORTANT: Do not modify ResourceAccess objects after adding them to the set. This will break the set.
        Set<ResourceAccess> resourceAccessSet = new HashSet<>();

        for (long adminPrincipalId : adminPrincipalIdSet) {
            ResourceAccess adminAccess = new ResourceAccess();
            adminAccess.setPrincipalId(adminPrincipalId);
            adminAccess.setAccessType(ModelConstants.ENTITY_ADMIN_ACCESS_PERMISSIONS);
            resourceAccessSet.add(adminAccess);
        }

        for (long readOnlyPrincipalId : readOnlyPrincipalIdSet) {
            ResourceAccess readOnlyAccess = new ResourceAccess();
            readOnlyAccess.setPrincipalId(readOnlyPrincipalId);
            readOnlyAccess.setAccessType(ACCESS_TYPE_READ);
            resourceAccessSet.add(readOnlyAccess);
        }

        AccessControlList acl = new AccessControlList();
        acl.setId(entityId);
        acl.setResourceAccess(resourceAccessSet);

        AccessControlList existingAcl = getAclWithRetry(entityId);
        if (existingAcl != null) {
            // We need to copy over the etag or else Synapse rejects the request.
            acl.setEtag(existingAcl.getEtag());
            return updateAclWithRetry(acl);
        } else {
            return createAclWithRetry(acl);
        }
    }

    /**
     * Creates an ACL in Synapse. This is a retry wrapper.
     *
     * @param acl
     *         ACL to create
     * @return created ACL
     * @throws SynapseException
     *         if the call fails
     */
    @RetryOnFailure(attempts = 2, delay = 100, unit = TimeUnit.MILLISECONDS, types = SynapseException.class,
            randomize = false)
    public AccessControlList createAclWithRetry(AccessControlList acl) throws SynapseException {
        rateLimiter.acquire();
        return synapseClient.createACL(acl);
    }

    /** Gets an ACL from Synapse. Returns null if it doesn't exist. This is a retry wrapper. */
    @RetryOnFailure(attempts = 2, delay = 100, unit = TimeUnit.MILLISECONDS, types = SynapseException.class,
            randomize = false)
    public AccessControlList getAclWithRetry(String entityId) throws SynapseException {
        rateLimiter.acquire();
        try {
            return synapseClient.getACL(entityId);
        } catch (SynapseNotFoundException ex) {
            return null;
        }
    }

    /** Updates an ACL in Synapse. This is a retry wrapper. */
    @RetryOnFailure(attempts = 2, delay = 100, unit = TimeUnit.MILLISECONDS, types = SynapseException.class,
            randomize = false)
    public AccessControlList updateAclWithRetry(AccessControlList acl) throws SynapseException {
        rateLimiter.acquire();
        return synapseClient.updateACL(acl);
    }

    /**
     * Convenience method to add annotations to an entity. If an annotation with the same key already exists, it will
     * be overwritten.
     */
    public Annotations addAnnotationsToEntity(String entityId, Map<String, AnnotationsValue> annotationMap)
            throws SynapseException {
        Annotations annotations = getAnnotationsWithRetry(entityId);
        annotations.getAnnotations().putAll(annotationMap);
        return updateAnnotationsWithRetry(entityId, annotations);
    }

    /** Get annotations for an entity. This is a retry wrapper. */
    @RetryOnFailure(attempts = 2, delay = 100, unit = TimeUnit.MILLISECONDS, types = SynapseException.class,
            randomize = false)
    public Annotations getAnnotationsWithRetry(String entityId) throws SynapseException {
        rateLimiter.acquire();
        return synapseClient.getAnnotationsV2(entityId);
    }

    /**
     * <p>
     * Update annotations on an entity. This is a retry wrapper.
     * </p>
     * <p>
     * Note that annotations exist on an entity by default. There is no create annotations API. To update annotations,
     * you need to call get annotations, then update annotations. Alternatively, call the convenience method
     * {@link #addAnnotationsToEntity}.
     * </p>
     */
    @RetryOnFailure(attempts = 2, delay = 100, unit = TimeUnit.MILLISECONDS, types = SynapseException.class,
            randomize = false)
    public Annotations updateAnnotationsWithRetry(String entityId, Annotations annotations) throws SynapseException {
        rateLimiter.acquire();
        return synapseClient.updateAnnotationsV2(entityId, annotations);
    }

    /** Looks up child by name. Returns null if the child doesn't exist. This is a retry wrapper. */
    @RetryOnFailure(attempts = 2, delay = 100, unit = TimeUnit.MILLISECONDS, types = SynapseException.class,
            randomize = false)
    public String lookupChildWithRetry(String parentId, String childName) throws SynapseException {
        rateLimiter.acquire();
        try {
            return synapseClient.lookupChild(parentId, childName);
        } catch (SynapseNotFoundException ex) {
            return null;
        }
    }

    /**
     * Creates column models in Synapse. This is a retry wrapper.
     *
     * @param columnList
     *         list of column models to create
     * @return created column models
     * @throws SynapseException
     *         if the call fails
     */
    @RetryOnFailure(attempts = 2, delay = 100, unit = TimeUnit.MILLISECONDS, types = SynapseException.class,
            randomize = false)
    public List<ColumnModel> createColumnModelsWithRetry(List<ColumnModel> columnList) throws SynapseException {
        rateLimiter.acquire();
        return synapseClient.createColumnModels(columnList);
    }

    /** Create entity in Synapse. This is a retry wrapper. */
    @RetryOnFailure(attempts = 2, delay = 100, unit = TimeUnit.MILLISECONDS, types = SynapseException.class,
            randomize = false)
    public <T extends Entity> T createEntityWithRetry(T entity) throws SynapseException {
        rateLimiter.acquire();
        return synapseClient.createEntity(entity);
    }

    /** Get entity in Synapse. Returns null if it doesn't exist. This is a retry wrapper. */
    @RetryOnFailure(attempts = 2, delay = 100, unit = TimeUnit.MILLISECONDS, types = SynapseException.class,
            randomize = false)
    public <T extends Entity> T getEntityWithRetry(String entityId, Class<T> entityClass) throws SynapseException {
        rateLimiter.acquire();
        try {
            return synapseClient.getEntity(entityId, entityClass);
        } catch (SynapseNotFoundException ex) {
            return null;
        }
    }

    /** Update entity, which is a PUT in Synapse. This is a retry wrapper. */
    @RetryOnFailure(attempts = 2, delay = 100, unit = TimeUnit.MILLISECONDS, types = SynapseException.class,
            randomize = false)
    public <T extends Entity> T updateEntityWithRetry(T entity) throws SynapseException {
        rateLimiter.acquire();
        return synapseClient.putEntity(entity);
    }

    /**
     * Uploads a file to Synapse as a file handle. This is a retry wrapper.
     *
     * @param file
     *         file to upload
     * @return file handle object from Synapse
     * @throws IOException
     *         if reading the file from disk fails
     * @throws SynapseException
     *         if the Synapse call fails
     */
    @RetryOnFailure(attempts = 2, delay = 1, unit = TimeUnit.SECONDS,
            types = { AmazonClientException.class, SynapseException.class }, randomize = false)
    public FileHandle createFileHandleWithRetry(File file) throws IOException, SynapseException {
        rateLimiter.acquire();
        return synapseClient.multipartUpload(file, null, null, null);
    }

    /**
     * Append rows to a table. This handles polling Synapse in a loop and error handling. Note that this method already
     * exists in SynapseClient.java, but we don't use it because we need to handle our own rate limiting.
     */
    public RowReferenceSet appendRowsToTable(AppendableRowSet rowSet, String tableId) throws BridgeSynapseException,
            SynapseException {
        String jobId = appendRowsToTableStart(rowSet, tableId);
        RowReferenceSet rowReferenceSet;
        for (int loops = 0; loops < asyncTimeoutLoops; loops++) {
            if (asyncIntervalMillis > 0) {
                try {
                    Thread.sleep(asyncIntervalMillis);
                } catch (InterruptedException ex) {
                    // noop
                }
            }

            // Poll.
            rowReferenceSet = appendRowsToTableGet(jobId, tableId);
            if (rowReferenceSet != null) {
                return rowReferenceSet;
            }

            // Result not ready. Loop around again.
        }

        // If we make it this far, this means we timed out.
        throw new BridgeSynapseException("Timed out appending rows to table " + tableId);
    }

    /** Starts async job to append rows to a table. */
    @RetryOnFailure(attempts = 2, delay = 100, unit = TimeUnit.MILLISECONDS, types = SynapseException.class,
            randomize = false)
    public String appendRowsToTableStart(AppendableRowSet rowSet, String tableId) throws SynapseException {
        rateLimiter.acquire();
        return synapseClient.appendRowSetToTableStart(rowSet, tableId);
    }

    /** Polls the result of an async job to append rows to a table. Returns null if the result is not ready. */
    @RetryOnFailure(attempts = 2, delay = 100, unit = TimeUnit.MILLISECONDS, types = SynapseException.class,
            randomize = false)
    public RowReferenceSet appendRowsToTableGet(String jobId, String tableId) throws SynapseException {
        try {
            rateLimiter.acquire();
            return synapseClient.appendRowSetToTableGet(jobId, tableId);
        } catch (SynapseResultNotReadyException ex) {
            // catch this and return null so we don't retry on "not ready"
            return null;
        }
    }

    /** Creates the S3 file handle in Synapse. This is a retry wrapper. */
    @RetryOnFailure(attempts = 2, delay = 100, unit = TimeUnit.MILLISECONDS, types = SynapseException.class,
            randomize = false)
    public S3FileHandle createS3FileHandleWithRetry(S3FileHandle s3FileHandle) throws SynapseException {
        rateLimiter.acquire();
        return synapseClient.createExternalS3FileHandle(s3FileHandle);
    }

    /**
     * Helper method to create a folder if the folder doesn't exist. Add a retry around this method in case of race
     * conditions between this server and another one.
     */
    @RetryOnFailure(attempts = 2, delay = 1, unit = TimeUnit.SECONDS, types = SynapseException.class,
            randomize = false)
    public String createFolderIfNotExists(String parentId, String folderName) throws SynapseException {
        String folderId = lookupChildWithRetry(parentId, folderName);
        if (folderId != null) {
            // Folder already exists. We don't need to do anything.
            return folderId;
        } else {
            Folder folder = new Folder();
            folder.setName(folderName);
            folder.setParentId(parentId);
            folder = createEntityWithRetry(folder);
            return folder.getId();
        }
    }

    /** Checks if Synapse is writable and throws if it isn't. */
    public void checkSynapseWritableOrThrow() throws BridgeSynapseException {
        boolean isSynapseWritable;
        try {
            isSynapseWritable = isSynapseWritable();
        } catch (SynapseException ex) {
            throw new BridgeSynapseException("Error calling Synapse: " + ex.getMessage(), ex);
        }
        if (!isSynapseWritable) {
            throw new BridgeSynapseException("Synapse not in writable state");
        }
    }

    /**
     * Gets the Synapse stack status and returns true if Synapse is up and in read/write state. Also includes retries.
     */
    @RetryOnFailure(attempts = 2, delay = 100, unit = TimeUnit.MILLISECONDS, types = SynapseException.class,
            randomize = false)
    public boolean isSynapseWritable() throws SynapseException {
        rateLimiter.acquire();
        StackStatus status = synapseClient.getCurrentStackStatus();
        return status.getStatus() == StatusEnum.READ_WRITE;
    }

    /** Create a project setting. This is a retry wrapper. */
    @RetryOnFailure(attempts = 2, delay = 100, unit = TimeUnit.MILLISECONDS, types = SynapseException.class,
            randomize = false)
    public ProjectSetting createProjectSettingWithRetry(ProjectSetting projectSetting)
            throws SynapseException {
        rateLimiter.acquire();
        return synapseClient.createProjectSetting(projectSetting);
    }

    /** Create a storage location in Synapse. This is a retry wrapper. */
    @RetryOnFailure(attempts = 2, delay = 100, unit = TimeUnit.MILLISECONDS, types = SynapseException.class,
            randomize = false)
    public <T extends StorageLocationSetting> T createStorageLocationWithRetry(T storageLocation)
            throws SynapseException {
        rateLimiter.acquire();
        return synapseClient.createStorageLocationSetting(storageLocation);
    }

    /**
     * Convenience method to creates a storage location and sets it to the given entity as the only storage location.
     * Storage locations can be applied to projects or folders.
     */
    public <T extends StorageLocationSetting> T createStorageLocationForEntity(String entityId, T storageLocation)
            throws SynapseException {
        // Create storage location.
        storageLocation = createStorageLocationWithRetry(storageLocation);

        // Add storage location to entity.
        UploadDestinationListSetting uploadDestination = new UploadDestinationListSetting();
        uploadDestination.setLocations(ImmutableList.of(storageLocation.getStorageLocationId()));
        uploadDestination.setProjectId(entityId);
        uploadDestination.setSettingsType(ProjectSettingsType.upload);
        createProjectSettingWithRetry(uploadDestination);

        return storageLocation;
    }

    /**
     * <p>
     * Create table in Synapse. This is a retry wrapper.
     * </p>
     * <p>
     * This is deprecated in favor of {@link #createEntityWithRetry}.
     * </p>
     *
     * @param table
     *         table to create
     * @return created table
     * @throws SynapseException
     *         if the Synapse call fails
     */
    @Deprecated
    @RetryOnFailure(attempts = 2, delay = 100, unit = TimeUnit.MILLISECONDS, types = SynapseException.class,
            randomize = false)
    public TableEntity createTableWithRetry(TableEntity table) throws SynapseException {
        rateLimiter.acquire();
        return synapseClient.createEntity(table);
    }

    /**
     * Get the column models for a Synapse table. This is a retry wrapper.
     *
     * @param tableId
     *         table to get column info for
     * @return list of columns
     * @throws SynapseException
     *         if the call fails
     */
    @RetryOnFailure(attempts = 2, delay = 100, unit = TimeUnit.MILLISECONDS, types = SynapseException.class,
            randomize = false)
    public List<ColumnModel> getColumnModelsForTableWithRetry(String tableId) throws SynapseException {
        getColumnModelsRateLimiter.acquire();
        return synapseClient.getColumnModelsForTableEntity(tableId);
    }

    /**
     * Gets a Synapse table. This is a retry wrapper.
     *
     * @param tableId
     *         table to get
     * @return table
     * @throws SynapseException
     *         if the Synapse call fails
     */
    @Deprecated
    @RetryOnFailure(attempts = 2, delay = 100, unit = TimeUnit.MILLISECONDS, types = SynapseException.class,
            randomize = false)
    public TableEntity getTableWithRetry(String tableId) throws SynapseException {
        rateLimiter.acquire();
        return synapseClient.getEntity(tableId, TableEntity.class);
    }

    /**
     * Starts a Synapse table transaction (for example, a schema update request). This is a retry wrapper.
     *
     * @param changeList
     *         changes to apply to table
     * @param tableId
     *         table to be changed
     * @return async job token
     * @throws SynapseException
     *         if the Synapse call fails
     */
    @RetryOnFailure(attempts = 2, delay = 100, unit = TimeUnit.MILLISECONDS, types = SynapseException.class,
            randomize = false)
    public String startTableTransactionWithRetry(List<TableUpdateRequest> changeList, String tableId)
            throws SynapseException {
        rateLimiter.acquire();
        return synapseClient.startTableTransactionJob(changeList, tableId);
    }

    /**
     * Polls Synapse to get the job status for a table transaction (such as a schema update request). If the job is not
     * ready, this will return null instead of throwing a SynapseResultNotReadyException. This is to prevent spurious
     * retries when a SynapseResultNotReadyException is thrown. This is a retry wrapper.
     *
     * @param jobToken
     *         job token from startTableTransactionWithRetry()
     * @param tableId
     *         table the job was working on
     * @return response from the table update
     * @throws SynapseException
     *         if the job fails
     */
    @RetryOnFailure(attempts = 2, delay = 100, unit = TimeUnit.MILLISECONDS, types = SynapseException.class,
            randomize = false)
    public List<TableUpdateResponse> getTableTransactionResultWithRetry(String jobToken, String tableId)
            throws SynapseException {
        try {
            rateLimiter.acquire();
            return synapseClient.getTableTransactionJobResults(jobToken, tableId);
        } catch (SynapseResultNotReadyException ex) {
            // catch this and return null so we don't retry on "not ready"
            return null;
        }
    }

    /**
     * Starts applying an uploaded TSV file handle to a Synapse table. This is a retry wrapper.
     *
     * @param tableId
     *         the table to apply the TSV to
     * @param fileHandleId
     *         the TSV file handle
     * @param tableDescriptor
     *         TSV table descriptor
     * @return an async job token
     * @throws SynapseException
     *         if the Synapse call fails
     */
    @RetryOnFailure(attempts = 2, delay = 100, unit = TimeUnit.MILLISECONDS, types = SynapseException.class,
            randomize = false)
    public String uploadTsvStartWithRetry(String tableId, String fileHandleId, CsvTableDescriptor tableDescriptor)
            throws SynapseException {
        rateLimiter.acquire();
        return synapseClient.uploadCsvToTableAsyncStart(tableId, fileHandleId, null, null, tableDescriptor, null);
    }

    /**
     * Polls Synapse to get the job status for the upload TSV to table job. If the job is not ready, this will return
     * null instead of throwing a SynapseResultNotReadyException. This is to prevent spurious retries when a
     * SynapseResultNotReadyException is thrown. This is a retry wrapper.
     *
     * @param jobToken
     *         job token from uploadTsvStartWithRetry()
     * @param tableId
     *         table the job was working on
     * @return upload table result object
     * @throws SynapseException
     *         if the job fails
     */
    @RetryOnFailure(attempts = 2, delay = 100, unit = TimeUnit.MILLISECONDS, types = SynapseException.class,
            randomize = false)
    public UploadToTableResult getUploadTsvStatus(String jobToken, String tableId) throws SynapseException {
        try {
            rateLimiter.acquire();
            return synapseClient.uploadCsvToTableAsyncGet(jobToken, tableId);
        } catch (SynapseResultNotReadyException ex) {
            // catch this and return null so we don't retry on "not ready"
            return null;
        }
    }

    /** Creates a Synapse team. This is a retry wrapper. */
    @RetryOnFailure(attempts = 2, delay = 100, unit = TimeUnit.MILLISECONDS, types = SynapseException.class,
            randomize = false)
    public Team createTeamWithRetry(Team team) throws SynapseException {
        rateLimiter.acquire();
        return synapseClient.createTeam(team);
    }
}
