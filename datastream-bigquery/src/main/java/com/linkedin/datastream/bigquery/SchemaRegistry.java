/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.bigquery;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.avro.Schema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;

import com.linkedin.datastream.common.VerifiableProperties;

/**
 * This class is a wrapper to schema registry client.
 */
public class SchemaRegistry {
    private static final Logger LOG = LoggerFactory.getLogger(SchemaRegistry.class);

    private static final String CONFIG_SCHEMA_REGISTRY_URL = "URL";
    private static final String CONFIG_SCHEMA_NAME_SUFFIX = "schemaNameSuffix";
    private static final String DEFAULT_CONFLUENT_SCHEMA_NAME_SUFFIX = "-value";

    private final KafkaAvroDeserializer _deserializer;
    private final String _schemaRegistryURL;
    private final String _schemaNameSuffix;
    private SchemaRegistryClient _schemaRegistryClient;

    /**
     * Constructor for SchemaRegistry.
     * @param props schema registry client properties.
     */
    public SchemaRegistry(VerifiableProperties props) {
        this._schemaRegistryURL = props.getString(CONFIG_SCHEMA_REGISTRY_URL);
        this._schemaRegistryClient = new CachedSchemaRegistryClient(_schemaRegistryURL, Integer.MAX_VALUE);
        this._schemaNameSuffix = props.getString(CONFIG_SCHEMA_NAME_SUFFIX, DEFAULT_CONFLUENT_SCHEMA_NAME_SUFFIX);
        this._deserializer = new KafkaAvroDeserializer(_schemaRegistryClient);
    }

    /**
     * Returns avro schema of a given topic.
     * @param topic topic name
     * @return avro schema
     */
    public Schema getSchemaByTopic(String topic) {
        try {
            String schemaName = topic + _schemaNameSuffix;
            return new Schema.Parser().parse(_schemaRegistryClient.getLatestSchemaMetadata(schemaName).getSchema());
        } catch (Exception e) {
            LOG.error("Unable to find schema for {} - {} : {}", _schemaRegistryURL, topic, e);
            throw new IllegalStateException("Avro schema not found for topic " + topic);
        }
    }

    /**
     * returns the deserializer which shares the underlying schema registry client
     * @return avro deserializer
     */
    public KafkaAvroDeserializer getDeserializer() {
        return _deserializer;
    }
}
