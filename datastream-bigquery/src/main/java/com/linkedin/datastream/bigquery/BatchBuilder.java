/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.bigquery;

import java.util.Map;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.DatastreamRecordMetadata;
import com.linkedin.datastream.common.Package;
import com.linkedin.datastream.metrics.DynamicMetricsManager;
import com.linkedin.datastream.serde.Deserializer;
import com.linkedin.datastream.server.api.transport.buffered.AbstractBatch;
import com.linkedin.datastream.server.api.transport.buffered.AbstractBatchBuilder;

/**
 * This class builds batches of records to be committed to BQ eventually.
 */
public class BatchBuilder extends AbstractBatchBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(BatchBuilder.class.getName());

    private final Function<BigqueryDatastreamDestination, Batch> newBatchSupplier;

    /**
     * Constructor for BatchBuilder
     * @param maxBatchSize any batch bigger than this threshold will be committed to BQ.
     * @param maxBatchAge any batch older than this threshold will be committed to BQ.
     * @param maxInflightCommits maximum allowed batches in the commit backlog.
     * @param committer committer object.
     * @param queueSize queue size of the batch builder.
     * @param valueDeserializer a Deserializer
     * @param destinationConfigurations a Map of BigqueryDatastreamDestination to BigqueryDatastreamConfiguration
     */
    BatchBuilder(final int maxBatchSize,
                 final int maxBatchAge,
                 final int maxInflightCommits,
                 final BigqueryBatchCommitter committer,
                 final int queueSize,
                 final Deserializer valueDeserializer,
                 final Map<BigqueryDatastreamDestination, BigqueryDatastreamConfiguration> destinationConfigurations) {
        super(queueSize);
        newBatchSupplier = destination -> new Batch(maxBatchSize,
                maxBatchAge,
                maxInflightCommits,
                valueDeserializer,
                committer,
                destinationConfigurations.get(destination).getSchemaEvolver()
        );
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
                            topicAndPartition -> newBatchSupplier.apply(BigqueryDatastreamDestination.parse(aPackage.getDestination())))
                            .write(aPackage);
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
            } catch (Exception e) {
                if (aPackage.isDataPackage()) {
                    DynamicMetricsManager.getInstance().createOrUpdateMeter(
                            this.getClass().getSimpleName(),
                            aPackage.getTopic(),
                            "errorCount",
                            1);
                    LOG.error("Unable to write to batch", e);
                    aPackage.getAckCallback().onCompletion(new DatastreamRecordMetadata(
                            aPackage.getCheckpoint(), aPackage.getTopic(), aPackage.getPartition()), e);
                } else {
                    LOG.error("Unable to process flush signal", e);
                }
            }
        }
        LOG.info("Batch builder stopped.");
    }
}
