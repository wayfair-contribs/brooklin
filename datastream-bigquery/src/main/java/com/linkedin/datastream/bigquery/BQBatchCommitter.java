package com.linkedin.datastream.bigquery;

import com.codahale.metrics.Meter;
import com.google.api.services.bigquery.model.TableDataInsertAllRequest;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Schema;
import com.linkedin.datastream.common.DatastreamRecordMetadata;
import com.linkedin.datastream.common.SendCallback;
import com.linkedin.datastream.common.VerifiableProperties;
import com.linkedin.datastream.metrics.DynamicMetricsManager;
import com.linkedin.datastream.server.api.transport.buffered.BatchCommitter;
import com.linkedin.datastream.server.api.transport.buffered.CommitCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class BQBatchCommitter implements BatchCommitter<TableDataInsertAllRequest.Rows> {
    private static final Logger LOG = LoggerFactory.getLogger(BQBatchCommitter.class.getName());

    private ConcurrentMap<String, Schema> destTableSchemas;
    private ConcurrentMap<String, Boolean> destTableCreated;

    private static final String CONFIG_THREADS = "threads";

    private final ExecutorService _executor;
    private final int _numOfCommitterThreads;

    private final BigQuery _bigquery;

    public BQBatchCommitter(VerifiableProperties properties) {
        String credentialsPath = properties.getString("credentialsPath");
        try {
            Credentials credentials = GoogleCredentials
                    .fromStream(new FileInputStream(credentialsPath));
            this._bigquery = BigQueryOptions.newBuilder().setCredentials(credentials).build().getService();
        } catch (FileNotFoundException e) {
            LOG.error("Credentials path {} does not exist", credentialsPath);
            throw new RuntimeException(e);
        } catch (IOException e) {
            LOG.error("Unable to read credentials: {}", credentialsPath);
            throw new RuntimeException(e);
        }


        this._numOfCommitterThreads = properties.getInt(CONFIG_THREADS, 1);
        this._executor = Executors.newFixedThreadPool(_numOfCommitterThreads);
    }

    public void setDestTableSchema(String dest, Schema schema) {
        destTableSchemas.putIfAbsent(dest, schema);
    }

    @Override
    public void commit(List<TableDataInsertAllRequest.Rows> batch,
                       String destination,
                       List<SendCallback> ackCallbacks,
                       List<DatastreamRecordMetadata> recordMetadata,
                       CommitCallback callback) {

    }

    @Override
    public void shutdown() {

    }

}
