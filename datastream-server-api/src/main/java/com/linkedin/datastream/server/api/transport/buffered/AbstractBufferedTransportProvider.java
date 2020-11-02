/*
 * Copyright 2020 Wayfair LLC. All rights reserved.
 * Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 * See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.api.transport.buffered;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

import com.linkedin.datastream.common.BrooklinEnvelope;
import com.linkedin.datastream.common.DatastreamRecordMetadata;
import com.linkedin.datastream.common.Package;
import com.linkedin.datastream.common.Record;
import com.linkedin.datastream.common.SendCallback;
import com.linkedin.datastream.server.DatastreamProducerRecord;
import com.linkedin.datastream.server.api.transport.TransportProvider;

/**
 * Extend this abstract class to implement buffered writes to the destination.
 */
public abstract class AbstractBufferedTransportProvider implements TransportProvider {
    protected static final Logger LOG = LoggerFactory.getLogger(AbstractBufferedTransportProvider.class.getName());

    protected static final String KAFKA_ORIGIN_TOPIC = "kafka-origin-topic";
    protected static final String KAFKA_ORIGIN_PARTITION = "kafka-origin-partition";
    protected static final String KAFKA_ORIGIN_OFFSET = "kafka-origin-offset";

    protected final String _transportProviderName;
    protected final ScheduledExecutorService _scheduler = new ScheduledThreadPoolExecutor(1);
    final protected ImmutableList<? extends AbstractBatchBuilder> _batchBuilders;

    static class DelayedCallback {
        final SendCallback callback;
        final DatastreamRecordMetadata metadata;
        final Exception exception;

        DelayedCallback(final SendCallback callback, final DatastreamRecordMetadata metadata, final Exception exception) {
            this.callback = callback;
            this.metadata = metadata;
            this.exception = exception;
        }
    }
    final ImmutableList<ConcurrentLinkedQueue<DelayedCallback>> delayedCallbackQueues;

    protected volatile boolean _isClosed;

    protected AbstractBufferedTransportProvider(final String transportProviderName, final List<? extends AbstractBatchBuilder> batchBuilders) {
        this._isClosed = false;
        this._transportProviderName = transportProviderName;
        _batchBuilders = ImmutableList.copyOf(batchBuilders);
        delayedCallbackQueues = ImmutableList.copyOf(IntStream.range(0, batchBuilders.size()).mapToObj(
                i -> new ConcurrentLinkedQueue<DelayedCallback>()
        ).collect(Collectors.toList()));
    }

    private void delegate(final com.linkedin.datastream.common.Package aPackage) {
        this._batchBuilders.get(Math.abs(aPackage.hashCode() % _batchBuilders.size())).assign(aPackage);
    }

    void fireDelayedCallbacks() {
        delayedCallbackQueues.forEach(queue -> {
            DelayedCallback delayedCallback;
            while ((delayedCallback = queue.poll()) != null) {
                delayedCallback.callback.onCompletion(delayedCallback.metadata, delayedCallback.exception);
            }
        });
    }

    void addDelayedCallback(final String topic, final String partition, final DelayedCallback delayedCallback) {
        delayedCallbackQueues.get(Math.abs(Objects.hash(topic, partition) % delayedCallbackQueues.size()))
                .add(delayedCallback);
    }

    @Override
    public void send(String destination, DatastreamProducerRecord record, SendCallback onComplete) {
        for (final BrooklinEnvelope env : record.getEvents()) {
            final String topic = env.getMetadata().get(KAFKA_ORIGIN_TOPIC);
            final String partition = env.getMetadata().get(KAFKA_ORIGIN_PARTITION);
            final String offset = env.getMetadata().get(KAFKA_ORIGIN_OFFSET);
            final Package aPackage = new Package.PackageBuilder()
                    .setRecord(new Record(env.getKey(), env.getValue()))
                    .setTopic(topic)
                    .setPartition(partition)
                    .setOffset(offset)
                    .setTimestamp(record.getEventsSourceTimestamp())
                    .setDestination(destination)
                    .setAckCallBack((final DatastreamRecordMetadata metadata, final Exception exception) -> {
                        addDelayedCallback(topic, partition, new DelayedCallback(onComplete, metadata, exception));
                    })
                    .setCheckpoint(record.getCheckpoint())
                    .build();
            delegate(aPackage);
        }
        fireDelayedCallbacks();
    }

    @Override
    public synchronized void close() {
        if (_isClosed) {
            LOG.info("Transport provider {} is already closed.", _transportProviderName);
            return;
        }

        try {
            LOG.info("Closing the transport provider {}", _transportProviderName);

            _scheduler.shutdown();
            try {
                _scheduler.awaitTermination(3, TimeUnit.SECONDS);
            } catch (final InterruptedException e) {
                LOG.warn("An interrupt was raised during awaitTermination() call on a ScheduledExecutorService");
                Thread.currentThread().interrupt();
            }

            for (AbstractBatchBuilder objectBuilder : _batchBuilders) {
                objectBuilder.shutdown();
            }

            for (AbstractBatchBuilder objectBuilder : _batchBuilders) {
                try {
                    objectBuilder.join();
                } catch (InterruptedException e) {
                    LOG.warn("An interrupt was raised during join() call on a Batch Builder");
                    Thread.currentThread().interrupt();
                }
            }

            shutdownCommitter();

            fireDelayedCallbacks();
        } finally {
            _isClosed = true;
        }
    }

    @Override
    public void flush() {
        LOG.info("Forcing flush on batch builders.");
        List<com.linkedin.datastream.common.Package> flushSignalPackages = new ArrayList<>();
        for (final AbstractBatchBuilder objectBuilder : _batchBuilders) {
            final Package aPackage = new Package.PackageBuilder().buildFroceFlushSignalPackage();
            flushSignalPackages.add(aPackage);
            objectBuilder.assign(aPackage);
        }
        for (final Package aPackage : flushSignalPackages) {
            aPackage.waitUntilDelivered();
        }
        fireDelayedCallbacks();
    }

    protected abstract void shutdownCommitter();
}
