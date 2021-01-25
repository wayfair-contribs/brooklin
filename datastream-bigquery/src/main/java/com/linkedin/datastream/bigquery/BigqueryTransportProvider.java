/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.bigquery;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
import com.linkedin.datastream.server.api.transport.TransportProvider;

import static com.linkedin.datastream.server.api.transport.buffered.AbstractBufferedTransportProvider.KAFKA_ORIGIN_CLUSTER;
import static com.linkedin.datastream.server.api.transport.buffered.AbstractBufferedTransportProvider.KAFKA_ORIGIN_OFFSET;
import static com.linkedin.datastream.server.api.transport.buffered.AbstractBufferedTransportProvider.KAFKA_ORIGIN_PARTITION;
import static com.linkedin.datastream.server.api.transport.buffered.AbstractBufferedTransportProvider.KAFKA_ORIGIN_TOPIC;

/**
 * This is a Bigquery Transport provider that writes events to specified bigquery table.
 */
public class BigqueryTransportProvider implements TransportProvider {

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

    private final BigqueryBufferedTransportProvider _bufferedTransportProvider;
    private final Serializer _valueSerializer;
    private final Deserializer _valueDeserializer;
    private final BigqueryDatastreamConfiguration _datastreamConfiguration;
    private final Map<BigqueryDatastreamDestination, BigqueryDatastreamConfiguration> _destinationConfigurations;
    private final Set<BigqueryDatastreamDestination> _destinations;

    /**
     * Constructor.
     * @param bufferedTransportProvider a BigqueryBufferedTransportProvider
     * @param valueSerializer a Serializer
     * @param valueDeserializer a Deserializer
     * @param datastreamConfiguration a BigqueryDatastreamConfiguration
     * @param destinationConfigurations a mapping of BigqueryDatastreamDestination to BigqueryDatastreamConfiguration
     */
    public BigqueryTransportProvider(final BigqueryBufferedTransportProvider bufferedTransportProvider,
                                     final Serializer valueSerializer,
                                     final Deserializer valueDeserializer,
                                     final BigqueryDatastreamConfiguration datastreamConfiguration,
                                     final Map<BigqueryDatastreamDestination, BigqueryDatastreamConfiguration> destinationConfigurations) {
        _bufferedTransportProvider = bufferedTransportProvider;
        _valueSerializer = valueSerializer;
        _valueDeserializer = valueDeserializer;
        _datastreamConfiguration = datastreamConfiguration;
        _destinationConfigurations = destinationConfigurations;
        _destinations = new HashSet<>();
    }

    @Override
    public void send(final String destination, final DatastreamProducerRecord record, final SendCallback onComplete) {
        final BigqueryDatastreamDestination datastreamDestination = BigqueryDatastreamDestination.parse(destination);
        registerConfigurationForDestination(datastreamDestination, _datastreamConfiguration);
        final SendCallback callbackHandler = _datastreamConfiguration.getDeadLetterTableConfiguration()
                .map(deadLetterTableConfiguration -> exceptionHandlingCallbackHandler(datastreamDestination, record, onComplete, deadLetterTableConfiguration))
                .orElse(onComplete);
        _bufferedTransportProvider.send(destination, record, callbackHandler);
    }

    private SendCallback exceptionHandlingCallbackHandler(final BigqueryDatastreamDestination datastreamDestination,
                                                          final DatastreamProducerRecord record,
                                                          final SendCallback onComplete,
                                                          final BigqueryDatastreamConfiguration deadLetterTableConfiguration) {
        return (metadata, exception) -> {
            if (exception == null || exception instanceof DatastreamTransientException) {
                onComplete.onCompletion(metadata, exception);
            } else {
                final BigqueryDatastreamDestination deadLetterTableDestination;
                if (deadLetterTableConfiguration.getDestination().isWildcardDestination()) {
                    deadLetterTableDestination = deadLetterTableConfiguration.getDestination().replaceWildcard(datastreamDestination.getDestinatonName());
                } else {
                    deadLetterTableDestination = deadLetterTableConfiguration.getDestination();
                }

                final DatastreamProducerRecord exceptionRecord;
                try {
                    exceptionRecord = createExceptionRecord(deadLetterTableDestination.getDestinatonName(), record, exception);
                } catch (final Exception e) {
                    LOG.error("Unable to create BigQuery exception record", e);
                    onComplete.onCompletion(metadata, e);
                    return;
                }
                registerConfigurationForDestination(deadLetterTableDestination, deadLetterTableConfiguration);
                // Send an exception record
                _bufferedTransportProvider.send(deadLetterTableDestination.toString(), exceptionRecord,
                        (exceptionRecordMetadata, exceptionRecordException) ->
                            onComplete.onCompletion(metadata,
                                    // Call the callback with the original exception if an exception is encountered
                                    // while trying to insert an exception record
                                    Optional.ofNullable(exceptionRecordException).map(e -> exception).orElse(null)
                            )
                );
            }
        };
    }

    private void registerConfigurationForDestination(final BigqueryDatastreamDestination destination, final BigqueryDatastreamConfiguration configuration) {
        if (_destinations.add(destination)) {
            _destinationConfigurations.put(destination, configuration);
        }
    }

    Set<BigqueryDatastreamDestination> getDestinations() {
        return Collections.unmodifiableSet(_destinations);
    }

    @Override
    public void close() {
    }

    @Override
    public void flush() {
        _bufferedTransportProvider.flush();
    }

    private DatastreamProducerRecord createExceptionRecord(final String exceptionsTopicName, final DatastreamProducerRecord record, final Exception exception) {
        final DatastreamProducerRecordBuilder exceptionRecordBuilder = new DatastreamProducerRecordBuilder();
        exceptionRecordBuilder.setSourceCheckpoint(record.getCheckpoint());
        exceptionRecordBuilder.setEventsSourceTimestamp(record.getEventsSourceTimestamp());
        record.getPartition().ifPresent(exceptionRecordBuilder::setPartition);
        record.getPartitionKey().ifPresent(exceptionRecordBuilder::setPartitionKey);
        record.getDestination().ifPresent(exceptionRecordBuilder::setDestination);

        final BigqueryExceptionType exceptionType = classifyException(exception);
        final String exceptionClassName = exception.getClass().getName();
        final String exceptionMessage = Optional.ofNullable(exception.getMessage()).orElse("");
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
            // Use a hard-coded topic name to serialize all exception records, so we only have to manage the exception record schema in one place
            final byte[] exceptionRecordBytes = _valueSerializer.serialize("brooklin-bigquery-transport-exceptions", exceptionAvroRecordBuilder.build());
            final Map<String, String> exceptionRecordMetadata = new HashMap<>(5);
            exceptionRecordMetadata.put(KAFKA_ORIGIN_CLUSTER, cluster);
            exceptionRecordMetadata.put(KAFKA_ORIGIN_TOPIC, exceptionsTopicName);
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

}
