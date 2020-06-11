package com.linkedin.datastream.server.api.transport.buffered;

import com.linkedin.datastream.common.BrooklinEnvelope;
import com.linkedin.datastream.common.BrooklinEnvelopeMetadataConstants;
import com.linkedin.datastream.common.Package;
import com.linkedin.datastream.common.Record;
import com.linkedin.datastream.common.SendCallback;
import com.linkedin.datastream.server.DatastreamProducerRecord;
import com.linkedin.datastream.server.api.transport.TransportProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public abstract class AbstractBufferedTransportProvider  implements TransportProvider {
    protected static final Logger LOG = LoggerFactory.getLogger(AbstractBufferedTransportProvider.class.getName());

    protected static final String KAFKA_ORIGIN_TOPIC = "kafka-origin-topic";
    protected static final String KAFKA_ORIGIN_PARTITION = "kafka-origin-partition";
    protected static final String KAFKA_ORIGIN_OFFSET = "kafka-origin-offset";

    protected String _transportProviderName;
    protected final ScheduledExecutorService _scheduler = new ScheduledThreadPoolExecutor(1);
    protected CopyOnWriteArrayList<AbstractBatchBuilder> _objectBuilders = new CopyOnWriteArrayList<>();

    protected volatile boolean _isClosed;

    protected AbstractBufferedTransportProvider(String transportProviderName) {
        this._isClosed = false;
        this._transportProviderName = transportProviderName;
    }

    private void delegate(final com.linkedin.datastream.common.Package aPackage) {
        this._objectBuilders.get(Math.abs(aPackage.hashCode() % _objectBuilders.size())).assign(aPackage);
    }

    @Override
    public void send(String destination, DatastreamProducerRecord record, SendCallback onComplete) {
        for (final BrooklinEnvelope env :  record.getEvents()) {
            final com.linkedin.datastream.common.Package aPackage = new com.linkedin.datastream.common.Package.PackageBuilder()
                    .setRecord(new Record(env.getKey(), env.getValue()))
                    .setTopic(env.getMetadata().get(KAFKA_ORIGIN_TOPIC))
                    .setPartition(env.getMetadata().get(KAFKA_ORIGIN_PARTITION))
                    .setOffset(env.getMetadata().get(KAFKA_ORIGIN_OFFSET))
                    .setTimestamp(env.getMetadata().get(BrooklinEnvelopeMetadataConstants.EVENT_TIMESTAMP))
                    .setDestination(destination)
                    .setAckCallBack(onComplete)
                    .setCheckpoint(record.getCheckpoint())
                    .build();
            delegate(aPackage);
        }
    }

    @Override
    public synchronized void close() {
        if (_isClosed) {
            LOG.info("Transport provider {} is already closed.", _transportProviderName);
            return;
        }

        try {
            LOG.info("Closing the transport provider {}", _transportProviderName);
            for (AbstractBatchBuilder objectBuilder : _objectBuilders) {
                objectBuilder.shutdown();
            }

            for (AbstractBatchBuilder objectBuilder : _objectBuilders) {
                try {
                    objectBuilder.join();
                } catch (InterruptedException e) {
                    LOG.warn("An interrupt was raised during join() call on a Object Builder");
                    Thread.currentThread().interrupt();
                }
            }

            shutdownCommitter();
        } finally {
            _isClosed = true;
        }
    }

    @Override
    public void flush() {
        LOG.info("Forcing flush on object builders.");
        List<com.linkedin.datastream.common.Package> flushSignalPackages = new ArrayList<>();
        for (final AbstractBatchBuilder objectBuilder : _objectBuilders) {
            final com.linkedin.datastream.common.Package aPackage = new com.linkedin.datastream.common.Package.PackageBuilder().buildFroceFlushSignalPackage();
            flushSignalPackages.add(aPackage);
            objectBuilder.assign(aPackage);
        }
        for (final Package aPackage : flushSignalPackages) {
            aPackage.waitUntilDelivered();
        }
    }

    protected abstract void shutdownCommitter();
}
