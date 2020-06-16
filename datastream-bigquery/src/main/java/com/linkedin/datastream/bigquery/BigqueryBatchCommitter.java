package com.linkedin.datastream.bigquery;

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.linkedin.datastream.common.DatastreamRecordMetadata;
import com.linkedin.datastream.common.DatastreamTransientException;
import com.linkedin.datastream.common.SendCallback;
import com.linkedin.datastream.common.VerifiableProperties;
import com.linkedin.datastream.server.api.transport.buffered.BatchCommitter;
import com.linkedin.datastream.server.api.transport.buffered.CommitCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class BigqueryBatchCommitter implements BatchCommitter<List<InsertAllRequest.RowToInsert>> {
    private static final Logger LOG = LoggerFactory.getLogger(BigqueryBatchCommitter.class.getName());

    private ConcurrentMap<String, Schema> _destTableSchemas;
    private Map<String, Boolean> _destTableCreated;

    private static final String CONFIG_THREADS = "threads";

    private final ExecutorService _executor;
    private final int _numOfCommitterThreads;

    private final BigQuery _bigquery;

    public BigqueryBatchCommitter(VerifiableProperties properties) {
        String credentialsPath = properties.getString("credentialsPath");
        String projectId = properties.getString("projectId");
        try {
            Credentials credentials = GoogleCredentials
                    .fromStream(new FileInputStream(credentialsPath));
            this._bigquery = BigQueryOptions.newBuilder()
                    .setProjectId(projectId)
                    .setCredentials(credentials).build().getService();
        } catch (FileNotFoundException e) {
            LOG.error("Credentials path {} does not exist", credentialsPath);
            throw new RuntimeException(e);
        } catch (IOException e) {
            LOG.error("Unable to read credentials: {}", credentialsPath);
            throw new RuntimeException(e);
        }


        this._numOfCommitterThreads = properties.getInt(CONFIG_THREADS, 1);
        this._executor = Executors.newFixedThreadPool(_numOfCommitterThreads);

        this._destTableSchemas = new ConcurrentHashMap<>();
        this._destTableCreated = new HashMap<>();
    }

    public void setDestTableSchema(String dest, Schema schema) {
        _destTableSchemas.putIfAbsent(dest, schema);
    }

    private synchronized void createTableIfAbsent(String destination) {
        if (_destTableCreated.containsKey(destination)) {
            return;
        }
        String[] datasetTable = destination.split("/");

        try {
            TableId tableId = TableId.of(datasetTable[0], datasetTable[1]);
            TableDefinition tableDefinition = StandardTableDefinition.of(_destTableSchemas.get(destination));
            TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
            if (_bigquery.getTable(tableId) != null) {
                LOG.info("Table {} already exist", destination);
                return;
            }
            _bigquery.create(tableInfo);
            LOG.info("Table {} created successfully", destination);
        } catch (BigQueryException e) {
            LOG.info("Failed to create table {}", destination);
            throw e;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void commit(List<InsertAllRequest.RowToInsert> batch,
                       String destination,
                       List<SendCallback> ackCallbacks,
                       List<DatastreamRecordMetadata> recordMetadata,
                       CommitCallback callback) {
        final Runnable committerTask = () -> {
            Exception exception = null;
            InsertAllResponse response = null;

            try {
                createTableIfAbsent(destination);

                String[] datasetTable = destination.split("/");
                TableId tableId = TableId.of(datasetTable[0], datasetTable[1]);

                response = _bigquery.insertAll(
                        InsertAllRequest.newBuilder(tableId, batch)
                                .build());
            } catch (Exception e) {
                LOG.warn("Failed to insert a rows {}", response);
                exception = new DatastreamTransientException(e);
            }

            for (int i = 0; i < ackCallbacks.size(); i++) {
                if (exception != null) {
                    ackCallbacks.get(i).onCompletion(recordMetadata.get(i), exception);
                    continue;
                }
                if (response != null && response.hasErrors()) {
                    exception = null;
                    Long key = new Long(i);
                    if (response.getInsertErrors().containsKey(key)) {
                        LOG.warn("Failed to insert a row {} {}", i, response.getInsertErrors().get(key));
                        exception = new DatastreamTransientException(response.getInsertErrors().get(key).toString());
                    }
                    ackCallbacks.get(i).onCompletion(recordMetadata.get(i), exception);
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
