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
import org.sagebionetworks.client.exceptions.SynapseResultNotReadyException;
import org.sagebionetworks.repo.model.ACCESS_TYPE;
import org.sagebionetworks.repo.model.AccessControlList;
import org.sagebionetworks.repo.model.ResourceAccess;
import org.sagebionetworks.repo.model.file.FileHandle;
import org.sagebionetworks.repo.model.table.ColumnChange;
import org.sagebionetworks.repo.model.table.ColumnModel;
import org.sagebionetworks.repo.model.table.ColumnType;
import org.sagebionetworks.repo.model.table.CsvTableDescriptor;
import org.sagebionetworks.repo.model.table.TableEntity;
import org.sagebionetworks.repo.model.table.TableSchemaChangeRequest;
import org.sagebionetworks.repo.model.table.TableSchemaChangeResponse;
import org.sagebionetworks.repo.model.table.TableUpdateRequest;
import org.sagebionetworks.repo.model.table.TableUpdateResponse;
import org.sagebionetworks.repo.model.table.UploadToTableResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.sagebionetworks.bridge.exceptions.BridgeSynapseException;
import org.sagebionetworks.bridge.exceptions.BridgeSynapseNonRetryableException;

/** Synapse operations that are common across multiple workers and should be shared. */
public class SynapseHelper {
    private static final Logger LOG = LoggerFactory.getLogger(SynapseHelper.class);

    private static final Joiner COMMA_SPACE_JOINER = Joiner.on(", ").useForNull("");

    // Package-scoped for unit tests.
    static final Set<ACCESS_TYPE> ACCESS_TYPE_ALL = ImmutableSet.copyOf(ACCESS_TYPE.values());
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
     * Helper method to create a table with the specified columns and set up ACLs. The data access team is set with
     * read permissions and the principal ID is set with all permissions.
     *
     * @param columnList
     *         list of column models to create on the table
     * @param dataAccessTeamId
     *         data access team ID, set with read permissions
     * @param principalId
     *         principal ID, set with all permissions
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
    public String createTableWithColumnsAndAcls(List<ColumnModel> columnList, long dataAccessTeamId,
            long principalId, String projectId, String tableName) throws BridgeSynapseException, SynapseException {
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
        // ResourceAccess is a mutable object, but the Synapse API takes them in a Set. This is a little weird.
        // IMPORTANT: Do not modify ResourceAccess objects after adding them to the set. This will break the set.
        Set<ResourceAccess> resourceAccessSet = new HashSet<>();

        ResourceAccess exporterOwnerAccess = new ResourceAccess();
        exporterOwnerAccess.setPrincipalId(principalId);
        exporterOwnerAccess.setAccessType(ACCESS_TYPE_ALL);
        resourceAccessSet.add(exporterOwnerAccess);

        ResourceAccess dataAccessTeamAccess = new ResourceAccess();
        dataAccessTeamAccess.setPrincipalId(dataAccessTeamId);
        dataAccessTeamAccess.setAccessType(ACCESS_TYPE_READ);
        resourceAccessSet.add(dataAccessTeamAccess);

        AccessControlList acl = new AccessControlList();
        acl.setId(synapseTableId);
        acl.setResourceAccess(resourceAccessSet);
        createAclWithRetry(acl);

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
     * Helper method to detect when a schema changes and updates the Synapse table accordingly. Will reject schema
     * changes that delete or modify columns. Optimized so if no columns were inserted, it won't modify the table.
     *
     * @param synapseTableId
     *         table to update
     * @param newColumnList
     *         columns to update the table with
     * @throws BridgeSynapseException
     *         if the column changes are incompatible
     * @throws SynapseException
     *         if the underlying Synapse calls fail
     */
    public void safeUpdateTable(String synapseTableId, List<ColumnModel> newColumnList) throws BridgeSynapseException,
            SynapseException {
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
            LOG.error("Table " + synapseTableId + " has deleted columns: " + COMMA_SPACE_JOINER.join(
                    deletedColumnNameSet));
            shouldThrow = true;
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
    @SuppressWarnings("UnusedParameters")
    public FileHandle createFileHandleWithRetry(File file) throws IOException, SynapseException {
        rateLimiter.acquire();
        return synapseClient.multipartUpload(file, null, null, null);
    }

    /**
     * Create table in Synapse. This is a retry wrapper.
     *
     * @param table
     *         table to create
     * @return created table
     * @throws SynapseException
     *         if the Synapse call fails
     */
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
}
