package com.linkedin.datastream.bigquery;

import java.util.Map;

import com.linkedin.datastream.common.DatastreamRecordMetadata;
import com.linkedin.datastream.common.Package;

import com.linkedin.datastream.common.VerifiableProperties;
import com.linkedin.datastream.server.api.transport.buffered.AbstractBatch;
import com.linkedin.datastream.server.api.transport.buffered.AbstractBatchBuilder;
import com.linkedin.datastream.server.api.transport.buffered.BatchCommitter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchBuilder extends AbstractBatchBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(BatchBuilder.class.getName());

    private final int _maxBatchSize;
    private final int _maxBatchAge;
    private final int _maxInflightCommits;
    private final VerifiableProperties _conversionProperties;
    private final BatchCommitter _batchCommitter;

    public BatchBuilder(int maxBatchSize,
                        int maxBatchAge,
                        int maxInflightCommits,
                        BatchCommitter batchCommitter,
                        int queueSize,
                        VerifiableProperties conversionProperties) {
        super(queueSize);
        this._maxBatchSize = maxBatchSize;
        this._maxBatchAge = maxBatchAge;
        this._maxInflightCommits = maxInflightCommits;
        this._conversionProperties = conversionProperties;
        this._batchCommitter = batchCommitter;
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
                            key -> new Batch(_maxBatchSize, _maxBatchAge, _maxInflightCommits, _conversionProperties, _batchCommitter)).write(aPackage);
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
