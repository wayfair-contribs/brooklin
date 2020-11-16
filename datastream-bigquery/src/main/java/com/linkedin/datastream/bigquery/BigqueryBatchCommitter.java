/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.bigquery;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.Optional;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
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
import com.linkedin.datastream.common.DatastreamTransientException;
import com.linkedin.datastream.common.SendCallback;
import com.linkedin.datastream.common.VerifiableProperties;
import com.linkedin.datastream.metrics.DynamicMetricsManager;
import com.linkedin.datastream.server.api.transport.buffered.BatchCommitter;
import com.linkedin.datastream.server.api.transport.buffered.CommitCallback;

/**
 * This class commits submitted batches to BQ tables.
 */
public class BigqueryBatchCommitter implements BatchCommitter<List<InsertAllRequest.RowToInsert>> {
    private static final Logger LOG = LoggerFactory.getLogger(BigqueryBatchCommitter.class.getName());

    private final ConcurrentMap<String, Schema> _destTableSchemas;
    private final ConcurrentMap<String, BigquerySchemaEvolver> _destTableSchemaEvolvers;

    private static final String CONFIG_THREADS = "threads";

    private final ExecutorService _executor;

    private final BigQuery _bigquery;

    private static String sanitizeTableName(String tableName) {
        return tableName.replaceAll("[^A-Za-z0-9_]+", "_");
    }

    /**
     * Constructor.
     * @param properties the VerifiableProperties
     */
    public BigqueryBatchCommitter(final VerifiableProperties properties) {
        this(constructClientFromProperties(properties), properties.getInt(CONFIG_THREADS, 1));
    }

    BigqueryBatchCommitter(final BigQuery bigQuery, final int numThreads) {
        this._bigquery = bigQuery;
        this._executor = Executors.newFixedThreadPool(numThreads);
        this._destTableSchemas = new ConcurrentHashMap<>();
        this._destTableSchemaEvolvers = new ConcurrentHashMap<>();
    }

    private static BigQuery constructClientFromProperties(final VerifiableProperties properties) {
        String credentialsPath = properties.getString("credentialsPath");
        String projectId = properties.getString("projectId");
        try {
            Credentials credentials = GoogleCredentials
                    .fromStream(new FileInputStream(credentialsPath));
            return BigQueryOptions.newBuilder()
                    .setProjectId(projectId)
                    .setCredentials(credentials).build().getService();
        } catch (FileNotFoundException e) {
            LOG.error("Credentials path {} does not exist", credentialsPath);
            throw new RuntimeException(e);
        } catch (IOException e) {
            LOG.error("Unable to read credentials: {}", credentialsPath);
            throw new RuntimeException(e);
        }
    }

    private synchronized void createOrUpdateTable(String destination) {
        final String[] datasetTableNameRetention = destination.split("/");

        final Schema desiredTableSchema = _destTableSchemas.get(destination);

        final long partitionRetentionDays = Long.parseLong(datasetTableNameRetention[2]);
        final TimePartitioning timePartitioning;
        if (partitionRetentionDays > 0) {
            timePartitioning = TimePartitioning.of(TimePartitioning.Type.DAY, partitionRetentionDays * 86400000L);
        } else {
            timePartitioning = TimePartitioning.of(TimePartitioning.Type.DAY);
        }

        final TableId tableId = TableId.of(datasetTableNameRetention[0], sanitizeTableName(datasetTableNameRetention[1]));

        final Optional<Table> optionalExistingTable = Optional.ofNullable(_bigquery.getTable(tableId));
        if (optionalExistingTable.isPresent()) {
            updateTable(destination, desiredTableSchema, timePartitioning, tableId, optionalExistingTable.get());
        } else {
            createTable(destination, desiredTableSchema, timePartitioning, tableId);
        }
    }

    private void updateTable(final String destination, final Schema desiredTableSchema,
                             final TimePartitioning timePartitioning, final TableId tableId, final Table existingTable) {
        final Schema existingTableSchema = Optional.ofNullable(existingTable.getDefinition().getSchema())
                .orElseThrow(() -> new IllegalStateException(String.format("schema not defined for table: %s", tableId)));
        if (!desiredTableSchema.equals(existingTableSchema)) {
            final Schema evolvedSchema = _destTableSchemaEvolvers.get(destination).evolveSchema(existingTableSchema, desiredTableSchema);
            if (!existingTableSchema.equals(evolvedSchema)) {
                try {
                    existingTable.toBuilder()
                            .setDefinition(createTableDefinition(evolvedSchema, timePartitioning))
                            .build().update();
                } catch (BigQueryException e) {
                    final Table currentTable = _bigquery.getTable(tableId);
                    final Schema currentTableSchema = currentTable.getDefinition().getSchema();
                    if (existingTableSchema.equals(currentTableSchema)) {
                        LOG.error("Failed to update table {}", destination, e);
                        throw e;
                    } else {
                        LOG.warn("Concurrent table schema update exception encountered for table {}. Retrying update with new base schema...", destination, e);
                        updateTable(destination, desiredTableSchema, timePartitioning, tableId, currentTable);
                    }
                }
                LOG.debug("Table {} updated with evolved schema", destination);
            }
        } else {
            LOG.debug("Table {} already exist", destination);
        }
    }

    private void createTable(final String destination, final Schema desiredTableSchema, final TimePartitioning timePartitioning, final TableId tableId) {
        final TableInfo tableInfo = TableInfo.newBuilder(tableId, createTableDefinition(desiredTableSchema, timePartitioning)).build();
        try {
            _bigquery.create(tableInfo);
        } catch (BigQueryException e) {
            LOG.warn("Failed to create table {}", destination, e);
            throw e;
        }
        LOG.info("Table {} created successfully", destination);
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
    public void setDestTableSchema(String dest, Schema schema) {
        _destTableSchemas.put(dest, schema);
    }

    public void setDestTableSchemaEvolver(final String dest, final BigquerySchemaEvolver schemaEvolver) {
        _destTableSchemaEvolvers.put(dest, schemaEvolver);
    }

    @Override
    public void commit(List<InsertAllRequest.RowToInsert> batch,
                       String destination,
                       List<SendCallback> ackCallbacks,
                       List<DatastreamRecordMetadata> recordMetadata,
                       List<Long> sourceTimestamps,
                       CommitCallback callback) {
        if (batch.isEmpty()) {
            return;
        }

        final Runnable committerTask = () -> {
            Exception exception = null;
            InsertAllResponse response = null;

            try {
                createOrUpdateTable(destination);

                TimeZone utcTimeZone = TimeZone.getTimeZone("UTC");
                SimpleDateFormat timeFmt = new SimpleDateFormat("yyyyMMdd");
                timeFmt.setTimeZone(utcTimeZone);

                String[] datasetTable = destination.split("/");
                TableId tableId = TableId.of(datasetTable[0],
                        sanitizeTableName(datasetTable[1]) + "$" + timeFmt.format(
                                Calendar.getInstance(utcTimeZone).getTimeInMillis()));

                LOG.debug("Committing a batch to dataset {} and table {}", datasetTable[0], sanitizeTableName(datasetTable[1]));
                long start = System.currentTimeMillis();

                response = _bigquery.insertAll(
                        InsertAllRequest.newBuilder(tableId, batch)
                                .build());

                DynamicMetricsManager.getInstance().createOrUpdateHistogram(
                        this.getClass().getSimpleName(),
                        recordMetadata.get(0).getTopic(),
                        "insertAllExecTime",
                         System.currentTimeMillis() - start);
            } catch (Exception e) {
                LOG.warn("Failed to insert a rows {}", response, e);
                exception = new DatastreamTransientException(e);
            }

            for (int i = 0; i < ackCallbacks.size(); i++) {
                if (exception != null) {
                    // entire batch failed
                    DynamicMetricsManager.getInstance().createOrUpdateMeter(
                            this.getClass().getSimpleName(),
                            recordMetadata.get(i).getTopic(),
                            "errorCount",
                            1);
                    ackCallbacks.get(i).onCompletion(recordMetadata.get(i), exception);
                } else {
                    Long key = Long.valueOf(i);
                    if (response != null && response.hasErrors() && response.getInsertErrors().containsKey(key)) {
                        LOG.warn("Failed to insert a row {} {}", i, response.getInsertErrors().get(key));
                        DynamicMetricsManager.getInstance().createOrUpdateMeter(
                                this.getClass().getSimpleName(),
                                recordMetadata.get(i).getTopic(),
                                "errorCount",
                                1);
                        ackCallbacks.get(i).onCompletion(recordMetadata.get(i),
                                new DatastreamTransientException(response.getInsertErrors().get(key).toString()));
                    } else {
                        DynamicMetricsManager.getInstance().createOrUpdateMeter(
                                this.getClass().getSimpleName(),
                                recordMetadata.get(i).getTopic(),
                                "commitCount",
                                1);
                        DynamicMetricsManager.getInstance().createOrUpdateHistogram(
                                this.getClass().getSimpleName(),
                                recordMetadata.get(i).getTopic(),
                                "eteLatency",
                                System.currentTimeMillis() - sourceTimestamps.get(i));
                        ackCallbacks.get(i).onCompletion(recordMetadata.get(i), null);
                    }
                }
            }

            callback.commited();
        };

        _executor.execute(committerTask);
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

}
