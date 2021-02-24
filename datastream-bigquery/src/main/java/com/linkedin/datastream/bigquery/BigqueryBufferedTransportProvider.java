/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.bigquery;

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.linkedin.datastream.server.api.transport.buffered.AbstractBatchBuilder;
import com.linkedin.datastream.server.api.transport.buffered.AbstractBufferedTransportProvider;

/**
 * Implementation of a Bigquery BufferedTransportProvider.
 */
public class BigqueryBufferedTransportProvider extends AbstractBufferedTransportProvider {

    private final BigqueryBatchCommitter _committer;
    private final int _maxBatchAge;

    /**
     * Constructor.
     * @param transportProviderName a String
     * @param batchBuilders a list of objects tha extend AbstractBatchBuilder
     * @param committer a BigqueryBatchCommitter
     * @param maxBatchAge an int
     */
    public BigqueryBufferedTransportProvider(final String transportProviderName,
                                             final List<? extends AbstractBatchBuilder> batchBuilders,
                                             final BigqueryBatchCommitter committer,
                                             final int maxBatchAge) {
        super(transportProviderName, batchBuilders);
        _committer = committer;
        _maxBatchAge = maxBatchAge;
        init();
    }

    private void init() {
        for (AbstractBatchBuilder batchBuilder : _batchBuilders) {
            batchBuilder.start();
        }

        // send periodic flush signal to commit stale objects
        _scheduler.scheduleAtFixedRate(
                () -> {
                    for (AbstractBatchBuilder objectBuilder: _batchBuilders) {
                        LOG.info("Try flush signal sent.");
                        objectBuilder.assign(new com.linkedin.datastream.common.Package.PackageBuilder().buildTryFlushSignalPackage());
                    }
                },
                _maxBatchAge / 2,
                _maxBatchAge / 2,
                TimeUnit.MILLISECONDS);
    }

    @Override
    protected void shutdownCommitter() {
        _committer.shutdown();
    }
}
