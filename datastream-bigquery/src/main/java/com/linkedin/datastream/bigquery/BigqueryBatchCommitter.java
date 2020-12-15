/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.bigquery;

import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TimePartitioning;

import com.linkedin.datastream.bigquery.schema.BigquerySchemaEvolver;
import com.linkedin.datastream.common.DatastreamRecordMetadata;
import com.linkedin.datastream.common.SendCallback;
import com.linkedin.datastream.metrics.DynamicMetricsManager;
import com.linkedin.datastream.server.api.transport.buffered.BatchCommitter;
import com.linkedin.datastream.server.api.transport.buffered.CommitCallback;

/**
 * This class commits submitted batches to BQ tables.
 */
public class BigqueryBatchCommitter implements BatchCommitter<List<InsertAllRequest.RowToInsert>> {
    private static final Logger LOG = LoggerFactory.getLogger(BigqueryBatchCommitter.class.getName());

    private final ConcurrentMap<BigqueryDatastreamDestination, Schema> _destTableSchemas;
    private final Map<BigqueryDatastreamDestination, BigqueryDatastreamConfiguration> _datastreamConfigurations;

    private final ExecutorService _executor;

    private final BigQuery _bigquery;

    private final DateTimeFormatter partitionDateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");

    /**
     * Constructor.
     * @param bigQuery a BigQuery instance
     * @param numThreads the number of committer threads
     */
    public BigqueryBatchCommitter(final BigQuery bigQuery, final int numThreads, final Map<BigqueryDatastreamDestination,
            BigqueryDatastreamConfiguration> datastreamConfigurations) {
        this._bigquery = bigQuery;
        this._executor = Executors.newFixedThreadPool(numThreads);
        this._destTableSchemas = new ConcurrentHashMap<>();
        _datastreamConfigurations = datastreamConfigurations;
    }

    private synchronized boolean createOrUpdateTable(final TableId tableId, final Schema desiredTableSchema,
                                                     final BigquerySchemaEvolver schemaEvolver, final Long partitionExpirationDays,
                                                     final List<BigqueryLabel> labels) {
        final TimePartitioning timePartitioning = Optional.ofNullable(partitionExpirationDays)
                .filter(partitionRetentionDays -> partitionRetentionDays > 0)
                .map(partitionRetentionDays -> TimePartitioning.of(TimePartitioning.Type.DAY, Duration.of(partitionRetentionDays, ChronoUnit.DAYS).toMillis()))
                .orElse(TimePartitioning.of(TimePartitioning.Type.DAY));

        final Optional<Table> optionalExistingTable = Optional.ofNullable(_bigquery.getTable(tableId));
        return optionalExistingTable.map(table -> updateTable(tableId, desiredTableSchema, timePartitioning, table, schemaEvolver, labels))
                .orElseGet(() -> createTable(tableId, desiredTableSchema, timePartitioning, labels));
    }

    private boolean updateTable(final TableId tableId, final Schema desiredTableSchema,
                             final TimePartitioning timePartitioning,  final Table existingTable,
                                final BigquerySchemaEvolver schemaEvolver,
                                final List<BigqueryLabel> labels) {
        final Schema existingTableSchema = Optional.ofNullable(existingTable.getDefinition().getSchema())
                .orElseThrow(() -> new IllegalStateException(String.format("schema not defined for table: %s", tableId)));
        final Set<BigqueryLabel> existingLabels = existingTable.getLabels().entrySet().stream()
                .map(entry -> new BigqueryLabel(entry.getKey(), entry.getValue())).collect(Collectors.toSet());
        final Optional<Map<String, String>> optionalLabelMap;
        if (!existingLabels.containsAll(labels)) {
            optionalLabelMap = Optional.of(labels.stream().collect(Collectors.toMap(BigqueryLabel::getName, BigqueryLabel::getValue)));
        } else {
            optionalLabelMap = Optional.empty();
        }
        final Optional<Schema> optionalEvolvedSchema;
        if (!desiredTableSchema.equals(existingTableSchema)) {
            final Schema evolvedSchema = schemaEvolver.evolveSchema(existingTableSchema, desiredTableSchema);
            if (!existingTableSchema.equals(evolvedSchema)) {
                optionalEvolvedSchema = Optional.of(evolvedSchema);
            } else {
                optionalEvolvedSchema = Optional.empty();
            }
        } else {
            optionalEvolvedSchema = Optional.empty();
        }
        boolean tableUpdated;
        if (optionalLabelMap.isPresent() || optionalEvolvedSchema.isPresent()) {
            try {
                final Table.Builder tableBuilder = existingTable.toBuilder();
                optionalEvolvedSchema.ifPresent(schema -> tableBuilder.setDefinition(createTableDefinition(schema, timePartitioning)));
                optionalLabelMap.ifPresent(tableBuilder::setLabels);
                tableBuilder.build().update();
                tableUpdated = true;
            } catch (BigQueryException e) {
                final Table currentTable = _bigquery.getTable(tableId);
                if (optionalEvolvedSchema.isPresent()) {
                    final Schema evolvedSchema = optionalEvolvedSchema.get();
                    final Schema currentTableSchema = currentTable.getDefinition().getSchema();
                    if (evolvedSchema.equals(currentTableSchema)) {
                        LOG.info("Schema already evolved for table {}", tableId);
                        tableUpdated = true;
                    } else if (!existingTableSchema.equals(currentTableSchema)) {
                        LOG.warn("Concurrent table schema update exception encountered for table {}. Retrying update with new base schema...", tableId, e);
                        tableUpdated = updateTable(tableId, desiredTableSchema, timePartitioning, currentTable, schemaEvolver, labels);
                    } else {
                        LOG.error("Failed to update schema for table {}", tableId, e);
                        throw e;
                    }
                } else {
                    final Set<BigqueryLabel> currentLabels = currentTable.getLabels().entrySet().stream()
                            .map(entry -> new BigqueryLabel(entry.getKey(), entry.getValue())).collect(Collectors.toSet());
                    if (currentLabels.containsAll(labels)) {
                        LOG.info("Labels already updated for table {}", tableId);
                        tableUpdated = true;
                    } else if (!existingTable.getLabels().equals(currentTable.getLabels())) {
                        LOG.warn("Concurrent table label update exception encountered for table {}. Retrying update...", tableId, e);
                        tableUpdated = updateTable(tableId, desiredTableSchema, timePartitioning, currentTable, schemaEvolver, labels);
                    } else {
                        LOG.error("Failed to update labels for table {}", tableId, e);
                        throw e;
                    }
                }
            }
        } else {
            LOG.debug("No update required for table {}", tableId);
            tableUpdated = false;
        }
        return tableUpdated;
    }

    private boolean createTable(final TableId tableId, final Schema desiredTableSchema, final TimePartitioning timePartitioning,
                                final List<BigqueryLabel> labels) {
        final Map<String, String> labelsMap = labels.stream().collect(Collectors.toMap(BigqueryLabel::getName, BigqueryLabel::getValue));
        final TableInfo tableInfo = TableInfo.newBuilder(tableId, createTableDefinition(desiredTableSchema, timePartitioning))
                .setLabels(labelsMap).build();
        try {
            _bigquery.create(tableInfo);
        } catch (BigQueryException e) {
            LOG.warn("Failed to create table {}", tableId, e);
            throw e;
        }
        LOG.info("Table {} created successfully", tableId);
        return true;
    }

    private static TableDefinition createTableDefinition(final Schema schema, final TimePartitioning timePartitioning) {
        return StandardTableDefinition.newBuilder()
                .setSchema(schema)
                .setTimePartitioning(timePartitioning)
                .build();
    }

    /**
     * Allows to submit table schema for a lazy auto table creation
     * @param dest dataset and table
     * @param schema table schema
     */
    public void setDestTableSchema(final BigqueryDatastreamDestination dest, final Schema schema) {
        _destTableSchemas.put(dest, schema);
    }

    @Override
    public void commit(List<InsertAllRequest.RowToInsert> batch,
                       final String destinationStr,
                       List<SendCallback> ackCallbacks,
                       List<DatastreamRecordMetadata> recordMetadata,
                       List<Long> sourceTimestamps,
                       CommitCallback callback) {
        if (batch.isEmpty()) {
            callback.commited();
            return;
        }

        final Runnable committerTask = () -> {
            final String classSimpleName = this.getClass().getSimpleName();

            final BigqueryDatastreamDestination destination = BigqueryDatastreamDestination.parse(destinationStr);
            final BigqueryDatastreamConfiguration datastreamConfiguration = Optional.ofNullable(_datastreamConfigurations.get(destination))
                    .orElseThrow(() -> new IllegalStateException(String.format("configuration not defined for destination: %s", destination)));
            final String tableName = sanitizeTableName(datastreamConfiguration.getTableNameTemplate()
                    .map(template -> String.format(template, destination.getDestinatonName()))
                    .orElse(destination.getDestinatonName()));
            final TableId tableId = TableId.of(destination.getProjectId(), destination.getDatasetId(), tableName);
            final String partition = partitionDateFormatter.format(LocalDate.now(ZoneOffset.UTC));
            final TableId insertTableId = TableId.of(tableId.getProject(), tableId.getDataset(), String.format("%s$%s", tableId.getTable(), partition));

            LOG.debug("Committing a batch to project {}, dataset {}, and table {}", tableId.getProject(), tableId.getDataset(), tableId.getTable());

            final long start = System.currentTimeMillis();
            Map<Integer, Exception> insertErrors = insertRowsAndMapErrorsWithRetry(insertTableId, batch);
            final long end = System.currentTimeMillis();
            DynamicMetricsManager.getInstance()
                    .createOrUpdateHistogram(this.getClass().getSimpleName(), recordMetadata.get(0).getTopic(), "insertAllExecTime", end - start);

            // If we manage the destination table and encountered insert errors, try creating/updating the destination table before retrying
            if (datastreamConfiguration.isManageDestinationTable() && !insertErrors.isEmpty()) {
                try {
                    final boolean tableUpdatedOrCreated = createOrUpdateTable(tableId, _destTableSchemas.get(destination),
                            datastreamConfiguration.getSchemaEvolver(), datastreamConfiguration.getPartitionExpirationDays().orElse(null),
                            datastreamConfiguration.getLabels());
                    if (tableUpdatedOrCreated) {
                        LOG.info("Table created/updated for destination {}. Retrying batch...", destination);
                        insertErrors = insertRowsAndMapErrorsWithRetry(insertTableId, batch);
                    }
                } catch (final Exception e) {
                    insertErrors = IntStream.range(0, batch.size()).boxed().collect(Collectors.toMap(i -> i, i -> e));
                }
            }

            final Map<Integer, Exception> finalInsertErrors = insertErrors;
            IntStream.range(0, ackCallbacks.size()).forEach(i -> {
                final DatastreamRecordMetadata currentRecordMetadata = recordMetadata.get(i);
                final SendCallback ackCallback = ackCallbacks.get(i);
                final String topic = currentRecordMetadata.getTopic();

                if (!finalInsertErrors.containsKey(i)) {
                    DynamicMetricsManager.getInstance().createOrUpdateMeter(classSimpleName, topic, "commitCount", 1);
                    final long currentRecordSourceTimestamp = sourceTimestamps.get(i);
                    DynamicMetricsManager.getInstance().createOrUpdateHistogram(classSimpleName, topic, "eteLatency",
                            System.currentTimeMillis() - currentRecordSourceTimestamp);
                    ackCallback.onCompletion(currentRecordMetadata, null);
                } else {
                    final Exception insertError = finalInsertErrors.get(i);
                    LOG.warn("Failed to insert a row {} {}", i, insertError.getMessage());
                    DynamicMetricsManager.getInstance().createOrUpdateMeter(classSimpleName, topic, "errorCount", 1);
                    ackCallback.onCompletion(currentRecordMetadata, insertError);
                }
            });

            callback.commited();
        };

        _executor.execute(committerTask);
    }

    Map<Integer, Exception> insertRowsAndMapErrorsWithRetry(final TableId insertTableId, final List<InsertAllRequest.RowToInsert> batch) {
        Map<Integer, Exception> insertErrors;
        if (batch.size() > 1) {
            try {
                final InsertAllResponse response = _bigquery.insertAll(InsertAllRequest.newBuilder(insertTableId, batch).build());
                insertErrors = response.getInsertErrors().entrySet().stream().collect(Collectors.toMap(
                        entry -> entry.getKey().intValue(),
                        entry -> new TransientStreamingInsertException(entry.getValue().toString())
                ));
            } catch (final Exception e) {
                if (isBatchSizeLimitException(e)) {
                    LOG.warn("Batch size limit hit for table {} with batch size of {}. Retrying with reduced batch sizes...", insertTableId, batch.size(), e);
                    final int halfIndex = batch.size() / 2;
                    final Map<Integer, Exception> firstBatchErrors = insertRowsAndMapErrorsWithRetry(insertTableId, batch.subList(0, halfIndex));
                    final Map<Integer, Exception> secondBatchErrors = insertRowsAndMapErrorsWithRetry(insertTableId, batch.subList(halfIndex, batch.size()));
                    insertErrors = new HashMap<>(firstBatchErrors);
                    for (Map.Entry<Integer, Exception> entry : secondBatchErrors.entrySet()) {
                        insertErrors.put(entry.getKey() + halfIndex, entry.getValue());
                    }
                } else {
                    final TransientStreamingInsertException wrappedException = new TransientStreamingInsertException(e);
                    insertErrors = IntStream.range(0, batch.size()).boxed().collect(Collectors.toMap(i -> i, i -> wrappedException));
                }
            }
        } else {
            insertErrors = insertRowsAndMapErrors(insertTableId, batch);
        }
        return insertErrors;
    }

    private Map<Integer, Exception> insertRowsAndMapErrors(final TableId insertTableId, final List<InsertAllRequest.RowToInsert> batch) {
        Map<Integer, Exception> insertErrors;
        if (!batch.isEmpty()) {
            try {
                final InsertAllResponse response = _bigquery.insertAll(InsertAllRequest.newBuilder(insertTableId, batch).build());
                insertErrors = response.getInsertErrors().entrySet().stream().collect(Collectors.toMap(
                        entry -> entry.getKey().intValue(),
                        entry -> new TransientStreamingInsertException(entry.getValue().toString())
                ));
            } catch (final Exception e) {
                final TransientStreamingInsertException wrappedException = new TransientStreamingInsertException(e);
                insertErrors = IntStream.range(0, batch.size()).boxed().collect(Collectors.toMap(i -> i, i -> wrappedException));
            }
        } else {
            insertErrors = Collections.emptyMap();
        }
        return insertErrors;
    }

    private static boolean isBatchSizeLimitException(final Exception e) {
        final String message = e.getMessage();
        return message != null &&
                (message.startsWith("Request payload size exceeds the limit") || message.startsWith("too many rows present in the request"));
    }

    @Override
    public void shutdown() {
        _executor.shutdown();
        try {
            if (!_executor.awaitTermination(5, TimeUnit.SECONDS)) {
                LOG.warn("Batch Committer shutdown timed out.");
            }
        } catch (InterruptedException e) {
            LOG.warn("Interrupted while awaiting committer termination.");
            Thread.currentThread().interrupt();
        }
        LOG.info("BQ Batch committer stopped.");
    }

    static String sanitizeTableName(String tableName) {
        return tableName.replaceAll("[^A-Za-z0-9_]+", "_");
    }

}
