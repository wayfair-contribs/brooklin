/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.bigquery;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.common.errors.SerializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.bigquery.schema.IncompatibleSchemaEvolutionException;
import com.linkedin.datastream.bigquery.translator.SchemaTranslationException;
import com.linkedin.datastream.common.BrooklinEnvelope;
import com.linkedin.datastream.common.BrooklinEnvelopeMetadataConstants;
import com.linkedin.datastream.common.DatastreamTransientException;
import com.linkedin.datastream.common.SendCallback;
import com.linkedin.datastream.serde.Deserializer;
import com.linkedin.datastream.server.DatastreamProducerRecord;
import com.linkedin.datastream.server.DatastreamProducerRecordBuilder;


import com.linkedin.datastream.server.api.transport.buffered.AbstractBatchBuilder;
import com.linkedin.datastream.server.api.transport.buffered.AbstractBufferedTransportProvider;

/**
 * This is a Bigquery Transport provider that writes events to specified bigquery table.
 */
public class BigqueryTransportProvider extends AbstractBufferedTransportProvider {

    private static final Logger LOG = LoggerFactory.getLogger(BigqueryTransportProvider.class.getName());

    static final Schema EXCEPTION_TYPE_SCHEMA = Schema.createEnum("exceptionType", null,
            "com.linkedin.datastream.bigquery",
            Arrays.stream(BigqueryExceptionType.values()).map(BigqueryExceptionType::name).collect(Collectors.toList()));

    static final Schema EXCEPTION_RECORD_SCHEMA = SchemaBuilder.builder("com.linkedin.datastream.bigquery")
            .record("ExceptionRecord").fields()
            .requiredString("kafkaCluster")
            .requiredString("kafkaTopic")
            .requiredString("kafkaPartition")
            .requiredString("kafkaOffset")
            .requiredString("kafkaEventTimestamp")
            .name("exceptionType").type(EXCEPTION_TYPE_SCHEMA).noDefault()
            .requiredString("exceptionClass")
            .requiredString("exceptionMessage")
            .requiredString("exceptionStackTrace")
            .requiredBytes("eventValueBytes")
            .optionalString("eventValueString")
            .endRecord();

    private final BigqueryBatchCommitter _committer;
    private final int _maxBatchAge;
    private final Serializer _valueSerializer;
    private final Deserializer _valueDeserializer;

    private BigqueryTransportProvider(final BigqueryTransportProviderBuilder builder) {
        super(builder._transportProviderName, builder._batchBuilders);
        _committer = builder._committer;
        _maxBatchAge = builder._maxBatchAge;
        _valueSerializer = builder._valueSerializer;
        _valueDeserializer = builder._valueDeserializer;
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
    public void send(final String destination, final DatastreamProducerRecord record, final SendCallback onComplete) {
        super.send(destination, record, ((metadata, exception) -> {
            if (exception == null) {
                onComplete.onCompletion(metadata, null);
            } else if (exception instanceof DatastreamTransientException) {
                onComplete.onCompletion(metadata, exception);
            } else {
                final DatastreamProducerRecord exceptionRecord;
                try {
                    exceptionRecord = createExceptionRecord(record, exception);
                } catch (final Exception e) {
                    LOG.error("Unable to create BigQuery exception record", e);
                    onComplete.onCompletion(metadata, e);
                    return;
                }
                // Send an exception record
                super.send(destination, exceptionRecord, (exceptionRecordMetadata, exceptionRecordException) ->
                        onComplete.onCompletion(metadata, exceptionRecordException));
            }
        }));
    }

    private DatastreamProducerRecord createExceptionRecord(final DatastreamProducerRecord record, final Exception exception) {
        final DatastreamProducerRecordBuilder exceptionRecordBuilder = new DatastreamProducerRecordBuilder();
        exceptionRecordBuilder.setSourceCheckpoint(record.getCheckpoint());
        exceptionRecordBuilder.setEventsSourceTimestamp(record.getEventsSourceTimestamp());
        record.getPartition().ifPresent(exceptionRecordBuilder::setPartition);
        record.getPartitionKey().ifPresent(exceptionRecordBuilder::setPartitionKey);
        record.getDestination().ifPresent(exceptionRecordBuilder::setDestination);

        final BigqueryExceptionType exceptionType = classifyException(exception);
        final String exceptionClassName = exception.getClass().getName();
        final String exceptionMessage = exception.getMessage();
        final String exceptionStackTrace = ExceptionUtils.getStackTrace(exception);
        record.getEventsSendTimestamp().ifPresent(exceptionRecordBuilder::setEventsSendTimestamp);
        record.getEvents().stream().map(event -> {
            final String cluster = event.getMetadata().get(KAFKA_ORIGIN_CLUSTER);
            final String topic = event.getMetadata().get(KAFKA_ORIGIN_TOPIC);
            final String partition = event.getMetadata().get(KAFKA_ORIGIN_PARTITION);
            final String offset = event.getMetadata().get(KAFKA_ORIGIN_OFFSET);
            final String eventTimestamp = event.getMetadata().get(BrooklinEnvelopeMetadataConstants.EVENT_TIMESTAMP);
            final GenericRecordBuilder exceptionAvroRecordBuilder = new GenericRecordBuilder(EXCEPTION_RECORD_SCHEMA)
                    .set("kafkaCluster", cluster)
                    .set("kafkaTopic", topic)
                    .set("kafkaPartition", partition)
                    .set("kafkaOffset", offset)
                    .set("kafkaEventTimestamp", eventTimestamp)
                    .set("exceptionType", new GenericData.EnumSymbol(EXCEPTION_TYPE_SCHEMA, exceptionType.name()))
                    .set("exceptionClass", exceptionClassName)
                    .set("exceptionMessage", exceptionMessage)
                    .set("exceptionStackTrace", exceptionStackTrace);
            if (event.value().isPresent()) {
                final byte[] valueBytes = (byte[]) event.value().get();
                exceptionAvroRecordBuilder.set("eventValueBytes", ByteBuffer.wrap(valueBytes));
                try {
                    final Object value = _valueDeserializer.deserialize(valueBytes);
                    exceptionAvroRecordBuilder.set("eventValueString", value.toString());
                } catch (final Exception e) {
                    LOG.warn("Unable to deserialize event value", e);
                }
            } else {
                exceptionAvroRecordBuilder.set("eventValueBytes", ByteBuffer.wrap(new byte[0]));
            }
            final byte[] exceptionRecordBytes = _valueSerializer.serialize("brooklin-bigquery-transport-exceptions", exceptionAvroRecordBuilder.build());
            final Map<String, String> exceptionRecordMetadata = new HashMap<>(5);
            exceptionRecordMetadata.put(KAFKA_ORIGIN_CLUSTER, cluster);
            exceptionRecordMetadata.put(KAFKA_ORIGIN_TOPIC, topic + "_exceptions");
            exceptionRecordMetadata.put(KAFKA_ORIGIN_PARTITION, partition);
            exceptionRecordMetadata.put(KAFKA_ORIGIN_OFFSET, offset);
            exceptionRecordMetadata.put(BrooklinEnvelopeMetadataConstants.EVENT_TIMESTAMP, eventTimestamp);
            return new BrooklinEnvelope(new byte[0], exceptionRecordBytes, exceptionRecordMetadata);
        }).forEachOrdered(exceptionRecordBuilder::addEvent);
        return exceptionRecordBuilder.build();
    }

    private static BigqueryExceptionType classifyException(final Exception e) {
        final BigqueryExceptionType exceptionType;
        if (e instanceof IncompatibleSchemaEvolutionException) {
            exceptionType = BigqueryExceptionType.SchemaEvolution;
        } else if (e instanceof TransientStreamingInsertException) {
            exceptionType = BigqueryExceptionType.InsertError;
        } else if (e instanceof SerializationException) {
            exceptionType = BigqueryExceptionType.Deserialization;
        } else if (e instanceof SchemaTranslationException) {
            exceptionType = BigqueryExceptionType.SchemaTranslation;
        } else {
            exceptionType = BigqueryExceptionType.Other;
        }
        return exceptionType;
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
        private int _maxBatchAge;
        private BigqueryBatchCommitter _committer;
        private List<BatchBuilder> _batchBuilders;
        private Serializer _valueSerializer;
        private Deserializer _valueDeserializer;

        /**
         * Set the name of the transport provider
         */
        public BigqueryTransportProviderBuilder setTransportProviderName(String transportProviderName) {
            this._transportProviderName = transportProviderName;
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
         * Set batch committer
         */
        public BigqueryTransportProviderBuilder setCommitter(BigqueryBatchCommitter committer) {
            this._committer = committer;
            return this;
        }

        /**
         * Set the batch builders
         * @param batchBuilders a list of BatchBuilder objects
         * @return the Builder
         */
        public BigqueryTransportProviderBuilder setBatchBuilders(final List<BatchBuilder> batchBuilders) {
            this._batchBuilders = batchBuilders;
            return this;
        }

        /**
         * Set the value serializer.
         * @param valueSerializer the Serializer
         * @return the Builder
         */
        public BigqueryTransportProviderBuilder setValueSerializer(final Serializer valueSerializer) {
            this._valueSerializer = valueSerializer;
            return this;
        }

        /**
         * Set the value deserializer.
         * @param valueDeserializer the Deserializer
         * @return the Builder
         */
        public BigqueryTransportProviderBuilder setValueDeserializer(final Deserializer valueDeserializer) {
            this._valueDeserializer = valueDeserializer;
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
