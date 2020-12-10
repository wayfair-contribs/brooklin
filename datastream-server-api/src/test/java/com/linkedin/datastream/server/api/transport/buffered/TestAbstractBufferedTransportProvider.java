/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.api.transport.buffered;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import com.linkedin.datastream.common.BrooklinEnvelope;
import com.linkedin.datastream.common.DatastreamRecordMetadata;
import com.linkedin.datastream.common.Package;
import com.linkedin.datastream.common.SendCallback;
import com.linkedin.datastream.server.DatastreamProducerRecordBuilder;

import static com.linkedin.datastream.server.api.transport.buffered.AbstractBufferedTransportProvider.KAFKA_ORIGIN_OFFSET;
import static com.linkedin.datastream.server.api.transport.buffered.AbstractBufferedTransportProvider.KAFKA_ORIGIN_PARTITION;
import static com.linkedin.datastream.server.api.transport.buffered.AbstractBufferedTransportProvider.KAFKA_ORIGIN_TOPIC;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

/**
 * Test AbstractBufferedTransportProvider.
 */
public class TestAbstractBufferedTransportProvider {

    private static class TestBufferedTransportProvider extends AbstractBufferedTransportProvider {

        TestBufferedTransportProvider(final String transportProviderName, final List<? extends AbstractBatchBuilder> batchBuilders) {
            super(transportProviderName, batchBuilders);
        }

        @Override
        protected void shutdownCommitter() {

        }
    }

    private static class TestBatchBuilder extends AbstractBatchBuilder {
        TestBatchBuilder(final int queueSize) {
            super(queueSize);
        }

        @Override
        public void assign(Package aPackage) {
            super.assign(aPackage);
            aPackage.markAsDelivered();
        }
    }

    @Test
    public void testDelayedCallbackOnFlush() throws InterruptedException {
        final AbstractBatchBuilder batchBuilder = new TestBatchBuilder(2);
        final AbstractBufferedTransportProvider transportProvider = new TestBufferedTransportProvider("test", ImmutableList.of(batchBuilder));
        final String destination = "test";

        final Object key = null;
        final Object value = null;
        final Map<String, String> metadata = ImmutableMap.of(
                KAFKA_ORIGIN_TOPIC, "topic",
                KAFKA_ORIGIN_PARTITION, "0",
                KAFKA_ORIGIN_OFFSET, "0"
        );
        final BrooklinEnvelope envelope = new BrooklinEnvelope(key, value, metadata);
        final long eventSourceTimestamp = Instant.now().toEpochMilli();
        final DatastreamProducerRecordBuilder recordBuilder = new DatastreamProducerRecordBuilder();
        recordBuilder.addEvent(envelope);
        recordBuilder.setEventsSourceTimestamp(eventSourceTimestamp);
        final SendCallback onComplete = mock(SendCallback.class);
        transportProvider.send(destination, recordBuilder.build(), onComplete);
        final Package aPackage = batchBuilder.getNextPackage();
        final DatastreamRecordMetadata recordMetadata = mock(DatastreamRecordMetadata.class);
        final Exception exception = mock(Exception.class);
        aPackage.getAckCallback().onCompletion(recordMetadata, exception);
        verifyZeroInteractions(onComplete);
        transportProvider.flush();
        verify(onComplete).onCompletion(recordMetadata, exception);
    }

    @Test
    public void testDelayedCallbackOnNextSend() throws InterruptedException {
        final AbstractBatchBuilder batchBuilder = new TestBatchBuilder(2);
        final AbstractBufferedTransportProvider transportProvider = new TestBufferedTransportProvider("test", ImmutableList.of(batchBuilder));
        final String destination = "test";

        final DatastreamProducerRecordBuilder recordBuilder1 = new DatastreamProducerRecordBuilder();
        recordBuilder1.addEvent(new BrooklinEnvelope(null, null, ImmutableMap.of(
                KAFKA_ORIGIN_TOPIC, "topic",
                KAFKA_ORIGIN_PARTITION, "0",
                KAFKA_ORIGIN_OFFSET, "0"
        )));
        recordBuilder1.setEventsSourceTimestamp(Instant.now().toEpochMilli());
        final SendCallback onComplete1 = mock(SendCallback.class);
        transportProvider.send(destination, recordBuilder1.build(), onComplete1);

        final Package aPackage = batchBuilder.getNextPackage();
        final DatastreamRecordMetadata recordMetadata = mock(DatastreamRecordMetadata.class);
        final Exception exception = mock(Exception.class);
        aPackage.getAckCallback().onCompletion(recordMetadata, exception);

        verifyZeroInteractions(onComplete1);

        final DatastreamProducerRecordBuilder recordBuilder2 = new DatastreamProducerRecordBuilder();
        recordBuilder2.addEvent(new BrooklinEnvelope(null, null, ImmutableMap.of(
                KAFKA_ORIGIN_TOPIC, "topic",
                KAFKA_ORIGIN_PARTITION, "0",
                KAFKA_ORIGIN_OFFSET, "1"
        )));
        recordBuilder2.setEventsSourceTimestamp(Instant.now().toEpochMilli());
        final SendCallback onCallback2 = mock(SendCallback.class);
        transportProvider.send(destination, recordBuilder2.build(), onCallback2);
        verify(onComplete1).onCompletion(recordMetadata, exception);
        verifyZeroInteractions(onCallback2);
    }

}
