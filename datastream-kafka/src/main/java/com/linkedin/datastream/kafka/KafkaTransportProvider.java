/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.kafka;


import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.Validate;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;

import com.linkedin.datastream.common.BrooklinEnvelope;
import com.linkedin.datastream.common.DatastreamRecordMetadata;
import com.linkedin.datastream.common.ErrorLogger;
import com.linkedin.datastream.common.ReflectionUtils;
import com.linkedin.datastream.common.SendCallback;
import com.linkedin.datastream.common.VerifiableProperties;
import com.linkedin.datastream.common.translator.RecordTranslator;
import com.linkedin.datastream.metrics.BrooklinMeterInfo;
import com.linkedin.datastream.metrics.BrooklinMetricInfo;
import com.linkedin.datastream.metrics.DynamicMetricsManager;
import com.linkedin.datastream.metrics.MetricsAware;
import com.linkedin.datastream.server.DatastreamProducerRecord;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.api.transport.TransportProvider;


/**
 * This is a Kafka Transport provider that writes events to Kafka.
 * It handles record translation and data movement to the Kafka producer.
 * @param <K> type of the key
 * @param <V> type of the value
 */
public class KafkaTransportProvider<K, V> implements TransportProvider {
  private static final String CLASS_NAME = KafkaTransportProvider.class.getSimpleName();
  private static final Logger LOG = LoggerFactory.getLogger(CLASS_NAME);

  static final String AGGREGATE = "aggregate";
  static final String EVENT_WRITE_RATE = "eventWriteRate";
  static final String EVENT_BYTE_WRITE_RATE = "eventByteWriteRate";
  static final String EVENT_TRANSPORT_ERROR_RATE = "eventTransportErrorRate";

  private final DatastreamTask _datastreamTask;
  private final List<KafkaProducerWrapper<K, V>> _producers;

  private final DynamicMetricsManager _dynamicMetricsManager;
  private final String _metricsNamesPrefix;
  private final String _keyTranslatorClass;
  private final String _valueTranslatorClass;
  private final boolean _translatorIncludeSchema;
  private final Meter _eventWriteRate;
  private final Meter _eventByteWriteRate;
  private final Meter _eventTransportErrorRate;

  /**
   * Constructor for KafkaTransportProvider.
   * @param datastreamTask the {@link DatastreamTask} to which this transport provider is being assigned
   * @param producers Kafka producers to use for producing data to destination Kafka cluster
   * @param props Kafka producer configuration
   * @param metricsNamesPrefix the prefix to use when emitting metrics
   * @throws IllegalArgumentException if either datastreamTask or producers is null
   * @throws com.linkedin.datastream.common.DatastreamRuntimeException if "bootstrap.servers" is not specified in the
   * supplied config
   * @see ProducerConfig
   */
  public KafkaTransportProvider(DatastreamTask datastreamTask, List<KafkaProducerWrapper<K, V>> producers,
      Properties props, String metricsNamesPrefix) {
    org.apache.commons.lang.Validate.notNull(datastreamTask, "null tasks");
    org.apache.commons.lang.Validate.notNull(producers, "null producer wrappers");
    _producers = producers;
    _datastreamTask = datastreamTask;
    VerifiableProperties kafkaTransportProviderProperties = new VerifiableProperties(props);
    LOG.info("Creating kafka transport provider with properties: {}", props);
    if (!kafkaTransportProviderProperties.containsKey(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
      String errorMessage = "Bootstrap servers are not set";
      ErrorLogger.logAndThrowDatastreamRuntimeException(LOG, errorMessage, null);
    }
    _keyTranslatorClass = kafkaTransportProviderProperties.getString(KafkaTransportProviderAdmin.CONFIG_KEY_TRANSLATOR,
            KafkaTransportProviderAdmin.DEFAULT_TRANSLATOR);
    _valueTranslatorClass = kafkaTransportProviderProperties.getString(KafkaTransportProviderAdmin.CONFIG_VALUE_TRANSLATOR,
            KafkaTransportProviderAdmin.DEFAULT_TRANSLATOR);
    _translatorIncludeSchema = kafkaTransportProviderProperties.getBoolean(KafkaTransportProviderAdmin.CONFIG_TRANSLATOR_INCLUDE_SCHEMA,
            false);
    // initialize metrics
    _dynamicMetricsManager = DynamicMetricsManager.getInstance();
    _metricsNamesPrefix = metricsNamesPrefix == null ? CLASS_NAME : metricsNamesPrefix + CLASS_NAME;
    _eventWriteRate = new Meter();
    _eventByteWriteRate = new Meter();
    _eventTransportErrorRate = new Meter();
  }

  public List<KafkaProducerWrapper<K, V>> getProducers() {
    return _producers;
  }

  @SuppressWarnings("unchecked")
  private ProducerRecord<K, V> convertToProducerRecord(String topicName,
      DatastreamProducerRecord record, Object event) throws Exception {

    Optional<Integer> partition = record.getPartition();
    RecordTranslator<?, GenericRecord> keyTranslator = ReflectionUtils.createInstance(_keyTranslatorClass);
    RecordTranslator<?, GenericRecord> valueTranslator = ReflectionUtils.createInstance(_valueTranslatorClass);
    K keyValue = null;
    V payloadValue = null;
    if (event instanceof BrooklinEnvelope) {
      BrooklinEnvelope envelope = (BrooklinEnvelope) event;
      if (envelope.key().isPresent()) {
        if (envelope.key().get() instanceof byte[]) {
          keyValue = (K) envelope.key().get();
        } else {
          keyValue = (K) keyTranslator.translateFromInternalFormat((GenericRecord) envelope.key().get(), false);
        }
      }

      if (envelope.value().isPresent()) {
        if (envelope.value().get() instanceof byte[]) {
          payloadValue = (V) envelope.value().get();
        } else {
          payloadValue = (V) valueTranslator.translateFromInternalFormat((GenericRecord) envelope.value().get(), _translatorIncludeSchema);
        }
      }
    }

    if (partition.isPresent() && partition.get() >= 0) {
      // If the partition is specified. We send the record to the specific partition
      return new ProducerRecord<>(topicName, partition.get(), keyValue, payloadValue);
    } else {
      // If the partition is not specified. We use the partitionKey as the key. Kafka will use the hash of that
      // to determine the partition. If partitionKey does not exist, use the key value.
      keyValue = record.getPartitionKey().isPresent()
              ? (K) record.getPartitionKey().get().getBytes(StandardCharsets.UTF_8) : keyValue;
      return new ProducerRecord<>(topicName, keyValue, payloadValue);
    }
  }

  @Override
  public void send(String destinationUri, DatastreamProducerRecord record, SendCallback onSendComplete) {
    String topicName = KafkaTransportProviderUtils.getTopicName(destinationUri);
    try {
      Validate.notNull(record, "null event record.");
      Validate.notNull(record.getEvents(), "null datastream events.");


      LOG.debug("Sending Datastream event record: {}", record);

      for (Object event : record.getEvents()) {
        ProducerRecord<K, V> outgoing = convertToProducerRecord(topicName, record, event);

        // Update topic-specific metrics and aggregate metrics
        _eventWriteRate.mark();

        KafkaProducerWrapper<K, V> producer =
            _producers.get(Math.abs(Objects.hash(outgoing.topic(), outgoing.partition())) % _producers.size());

        producer.send(_datastreamTask, outgoing, (metadata, exception) -> {
          int partition = metadata != null ? metadata.partition() : -1;
          if (exception != null) {
            LOG.error("Sending a message with source checkpoint {} to topic {} partition {} for datastream task {} "
                    + "threw an exception.", record.getCheckpoint(), topicName, partition, _datastreamTask, exception);
          }
          doOnSendCallback(record, onSendComplete, metadata, exception);
        });

        _dynamicMetricsManager.createOrUpdateMeter(_metricsNamesPrefix, topicName, EVENT_WRITE_RATE, 1);
        _dynamicMetricsManager.createOrUpdateMeter(_metricsNamesPrefix, AGGREGATE, EVENT_WRITE_RATE, 1);
      }
    } catch (Exception e) {
      _eventTransportErrorRate.mark();
      _dynamicMetricsManager.createOrUpdateMeter(_metricsNamesPrefix, topicName, EVENT_TRANSPORT_ERROR_RATE, 1);
      String errorMessage = String.format(
          "Sending DatastreamRecord (%s) to topic %s, partition %s, Kafka cluster %s failed with exception.", record,
          topicName, record.getPartition().orElse(-1), destinationUri);

      ErrorLogger.logAndThrowDatastreamRuntimeException(LOG, errorMessage, e);
    }

    LOG.debug("Done sending Datastream event record: {}", record);
  }

  @Override
  public void close() {
    _producers.forEach(p -> p.close(_datastreamTask));
  }

  @Override
  public void flush() {
    _producers.forEach(KafkaProducerWrapper::flush);
  }

  private void doOnSendCallback(DatastreamProducerRecord record, SendCallback onComplete, RecordMetadata metadata,
      Exception exception) {
    if (onComplete != null) {
      onComplete.onCompletion(
          metadata != null ? new DatastreamRecordMetadata(record.getCheckpoint(), metadata.topic(),
              metadata.partition()) : null, exception);
    }
  }

  /**
   * Get the metrics info for a given metrics name prefix.
   * @param metricsNamesPrefix metrics name prefix to look up metrics info for.
   * @return the list of {@link BrooklinMetricInfo} found for the metrics name prefix
   */
  public static List<BrooklinMetricInfo> getMetricInfos(String metricsNamesPrefix) {
    String prefix = metricsNamesPrefix == null ? CLASS_NAME + MetricsAware.KEY_REGEX
        : metricsNamesPrefix + CLASS_NAME + MetricsAware.KEY_REGEX;

    List<BrooklinMetricInfo> metrics = new ArrayList<>();
    metrics.add(new BrooklinMeterInfo(prefix + EVENT_WRITE_RATE));
    metrics.add(new BrooklinMeterInfo(prefix + EVENT_BYTE_WRITE_RATE));
    metrics.add(new BrooklinMeterInfo(prefix + EVENT_TRANSPORT_ERROR_RATE));

    return Collections.unmodifiableList(metrics);
  }
}
