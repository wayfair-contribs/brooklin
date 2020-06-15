package com.linkedin.datastream.common;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SchemaRegistry {
    private static final Logger LOG = LoggerFactory.getLogger(SchemaRegistry.class);
    private final static Map<String, Schema> SCHEMAS = new ConcurrentHashMap<>();

    private static final String CONFIG_SCHEMA_REGISTRY_URL = "URL";
    private static final String CONFIG_SCHEMA_NAME_SUFFIX = "schemaNameSuffix";
    private static final String DEFAULT_CONFLUENT_SCHEMA_NAME_SUFFIX = "-value";

    private KafkaAvroDeserializer _deserializer;
    private String _schemaRegistryURL;
    private String _schemaNameSuffix;
    private SchemaRegistryClient _schemaRegistryClient;

    public SchemaRegistry(VerifiableProperties props) {
        this._schemaRegistryURL = props.getString(CONFIG_SCHEMA_REGISTRY_URL);
        this._schemaRegistryClient = new CachedSchemaRegistryClient(_schemaRegistryURL, Integer.MAX_VALUE);
        this._schemaNameSuffix = props.getString(CONFIG_SCHEMA_NAME_SUFFIX, DEFAULT_CONFLUENT_SCHEMA_NAME_SUFFIX);
        this._deserializer = new KafkaAvroDeserializer(_schemaRegistryClient);
    }

    public Schema getSchemaByTopic(String topic) {
        String key = _schemaRegistryURL + "-" + topic;
        Schema schema =  SCHEMAS.computeIfAbsent(key, (k) -> {
            try {
                String schemaName = topic + _schemaNameSuffix;
                return new Schema.Parser().parse(_schemaRegistryClient.getLatestSchemaMetadata(schemaName).getSchema());
            } catch (Exception e) {
                LOG.error("Unable to find schema for {} - {}", key, e);
                return null;
            }
        });

        if (schema == null) {
            throw new IllegalStateException("Avro schema not found for topic " + topic);
        }
        return schema;
    }

    public KafkaAvroDeserializer getDeserializer() {
        return _deserializer;
    }
}
