package com.linkedin.datastream.bigquery;

import java.util.Map;

import com.linkedin.datastream.common.DatastreamRecordMetadata;
import com.linkedin.datastream.common.Package;

import com.linkedin.datastream.common.SchemaRegistry;
import com.linkedin.datastream.common.VerifiableProperties;
import com.linkedin.datastream.server.api.transport.buffered.AbstractBatch;
import com.linkedin.datastream.server.api.transport.buffered.AbstractBatchBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchBuilder extends AbstractBatchBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(BatchBuilder.class.getName());
    private static final String CONFIG_SCHEMA_REGISTRY = "schemaRegistry";

    private final int _maxBatchSize;
    private final int _maxBatchAge;
    private final int _maxInflightCommits;
    private final BigqueryBatchCommitter _batchCommitter;
    private final SchemaRegistry _schemaRegistry;

    public BatchBuilder(int maxBatchSize,
                        int maxBatchAge,
                        int maxInflightCommits,
                        BigqueryBatchCommitter batchCommitter,
                        int queueSize,
                        VerifiableProperties translatorProperties) {
        super(queueSize);
        this._maxBatchSize = maxBatchSize;
        this._maxBatchAge = maxBatchAge;
        this._maxInflightCommits = maxInflightCommits;
        this._batchCommitter = batchCommitter;
        this._schemaRegistry = new SchemaRegistry(
                new VerifiableProperties(translatorProperties.getDomainProperties(CONFIG_SCHEMA_REGISTRY)));
    }

    @Override
    public void run() {
        while (!isInterrupted()) {

            Package aPackage;
            try {
                aPackage = getNextPackage();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }

            try {
                if (aPackage.isDataPackage()) {
                    _registry.computeIfAbsent(aPackage.getTopic() + "-" + aPackage.getPartition(),
                            key -> new Batch(_maxBatchSize, _maxBatchAge, _maxInflightCommits, _schemaRegistry, _batchCommitter)).write(aPackage);
                } else {
                    // broadcast signal
                    for (Map.Entry<String, AbstractBatch> entry : _registry.entrySet()) {
                        entry.getValue().write(aPackage);
                    }
                }
                aPackage.markAsDelivered();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (IllegalStateException e) {
                LOG.error("Unable to write to WriteLog {}", e);
                aPackage.getAckCallback().onCompletion(new DatastreamRecordMetadata(
                        aPackage.getCheckpoint(), aPackage.getTopic(), aPackage.getPartition()), e);
            }
        }
        LOG.info("Batch builder stopped.");
    }
}
