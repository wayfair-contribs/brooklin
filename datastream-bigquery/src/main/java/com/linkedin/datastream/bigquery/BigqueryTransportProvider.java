/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.bigquery;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.api.services.bigquery.model.TableDataInsertAllRequest;

import com.linkedin.datastream.common.VerifiableProperties;
import com.linkedin.datastream.server.api.transport.buffered.AbstractBatchBuilder;
import com.linkedin.datastream.server.api.transport.buffered.AbstractBufferedTransportProvider;
import com.linkedin.datastream.server.api.transport.buffered.BatchCommitter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a Bigquery Transport provider that writes events to specified bigquery table.
 */
public class BigqueryTransportProvider extends AbstractBufferedTransportProvider {

    private static final Logger LOG = LoggerFactory.getLogger(BigqueryTransportProvider.class.getName());

    private static final String KAFKA_ORIGIN_TOPIC = "kafka-origin-topic";
    private static final String KAFKA_ORIGIN_PARTITION = "kafka-origin-partition";
    private static final String KAFKA_ORIGIN_OFFSET = "kafka-origin-offset";

    private String _transportProviderName;
    private final ScheduledExecutorService _scheduler = new ScheduledThreadPoolExecutor(1);
    private CopyOnWriteArrayList<BatchBuilder> _batchBuilders = new CopyOnWriteArrayList<>();
    private final BatchCommitter<TableDataInsertAllRequest.Rows> _committer;

    private volatile boolean _isClosed;

    private BigqueryTransportProvider(BigqueryTransportProviderBuilder builder) {
        super(builder._transportProviderName);

        this._committer = builder._committer;

        // initialize and start object builders
        for (int i = 0; i < builder._batchBuilderCount; i++) {
            _objectBuilders.add(new BatchBuilder(
                    builder._maxBatchSize,
                    builder._maxBatchAge,
                    builder._maxInflightBatchCommits,
                    builder._committer,
                    builder._batchBuilderQueueSize,
                    builder._conversionProperties));
        }
        for (AbstractBatchBuilder batchBuilder : _batchBuilders) {
            batchBuilder.start();
        }

        // send periodic flush signal to commit stale objects
        _scheduler.scheduleAtFixedRate(
                () -> {
                    for (AbstractBatchBuilder objectBuilder: _objectBuilders) {
                        LOG.info("Try flush signal sent.");
                        objectBuilder.assign(new com.linkedin.datastream.common.Package.PackageBuilder().buildTryFlushSignalPackage());
                    }
                },
                builder._maxBatchAge / 2,
                builder._maxBatchAge / 2,
                TimeUnit.MILLISECONDS);
    }

    @Override
    protected void shutdownCommitter() {
        _committer.shutdown();
    }

    /**
     * Builder class for {@link com.linkedin.datastream.bigquery.BigqueryTransportProvider}
     */
    public static class BigqueryTransportProviderBuilder {
        private String _transportProviderName;
        private int _batchBuilderQueueSize;
        private int _batchBuilderCount;
        private int _maxBatchSize;
        private int _maxBatchAge;
        private int _maxInflightBatchCommits;
        private BatchCommitter _committer;
        private VerifiableProperties _conversionProperties;

        /**
         * Set the name of the transport provider
         */
        public BigqueryTransportProviderBuilder setTransportProviderName(String transportProviderName) {
            this._transportProviderName = transportProviderName;
            return this;
        }

        /**
         * Set batch builder's queue size
         */
        public BigqueryTransportProviderBuilder setBatchBuilderQueueSize(int batchBuilderQueueSize) {
            this._batchBuilderQueueSize = batchBuilderQueueSize;
            return this;
        }

        /**
         * Set number of batch builders
         */
        public BigqueryTransportProviderBuilder setBatchBuilderCount(int batchBuilderCount) {
            this._batchBuilderCount = batchBuilderCount;
            return this;
        }

        /**
         * Set max batch size
         */
        public BigqueryTransportProviderBuilder setMaxBatchSize(int maxBatchSize) {
            this._maxBatchSize = maxBatchSize;
            return this;
        }

        /**
         * Set max batch age
         */
        public BigqueryTransportProviderBuilder setMaxBatchAge(int maxBatchAge) {
            this._maxBatchAge = maxBatchAge;
            return this;
        }

        /**
         * Set max inflight commits
         */
        public BigqueryTransportProviderBuilder setMaxInflightBatchCommits(int maxInflightBatchCommits) {
            this._maxInflightBatchCommits = maxInflightBatchCommits;
            return this;
        }

        /**
         * Set batch committer
         */
        public BigqueryTransportProviderBuilder setCommitter(BatchCommitter<TableDataInsertAllRequest.Rows> committer) {
            this._committer = committer;
            return this;
        }

        public BigqueryTransportProviderBuilder setConversionProperties(VerifiableProperties conversionProperties) {
            this._conversionProperties = conversionProperties;
            return this;
        }

        /**
         * Build the BigqueryTransportProvider.
         * @return
         *   BigqueryTransportProvider that is created.
         */
        public BigqueryTransportProvider build() {
            return new BigqueryTransportProvider(this);
        }
    }


}
