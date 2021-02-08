/*
 * Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */

package com.linkedin.datastream.bigquery;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

import com.linkedin.datastream.bigquery.schema.BigquerySchemaEvolver;
import com.linkedin.datastream.bigquery.schema.FixedBigquerySchemaEvolver;
import com.linkedin.datastream.common.DatastreamRuntimeException;

/**
 * A factory for creating BigqueryDatastreamConfiguration.
 */
public class BigqueryDatastreamConfigurationFactory {

    private final Logger logger = LoggerFactory.getLogger(BigqueryDatastreamConfigurationFactory.class);

    /**
     * Create a BigqueryDatastreamConfiguration.
     * @param destination
     * @param datastreamName
     * @param schemaRegistryLocation
     * @param schemaEvolver
     * @param autoCreateTable
     * @param partitionExpirationDays
     * @param deadLetterTableConfiguration
     * @param labels
     * @param schemaId
     * @return the BigqueryDatastreamConfiguration
     */
    public BigqueryDatastreamConfiguration createBigqueryDatastreamConfiguration(
            final BigqueryDatastreamDestination destination,
            final String datastreamName,
            final String schemaRegistryLocation,
            final BigquerySchemaEvolver schemaEvolver,
            final boolean autoCreateTable,
            final Long partitionExpirationDays,
            final BigqueryDatastreamConfiguration deadLetterTableConfiguration,
            final List<BigqueryLabel> labels,
            final Integer schemaId) {
        final CachedSchemaRegistryClient confluentSchemaRegistryClient = new CachedSchemaRegistryClient(
                schemaRegistryLocation,
                AbstractKafkaAvroSerDeConfig.MAX_SCHEMAS_PER_SUBJECT_DEFAULT
        );

        final Map<String, Object> valueSerDeConfig = new HashMap<>();
        valueSerDeConfig.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryLocation);

        final KafkaSerializer valueSerializer = new KafkaSerializer(new KafkaAvroSerializer(confluentSchemaRegistryClient, valueSerDeConfig));
        final KafkaDeserializer valueDeserializer = new KafkaDeserializer(new KafkaAvroDeserializer(confluentSchemaRegistryClient, valueSerDeConfig));


        final BigqueryDatastreamConfiguration.Builder configBuilder = BigqueryDatastreamConfiguration.builder(
                destination,
                schemaEvolver,
                autoCreateTable,
                valueDeserializer,
                valueSerializer
        );

        if (schemaEvolver instanceof FixedBigquerySchemaEvolver) {
            if (schemaId == null) {
                throw new IllegalArgumentException("schema ID is required for fixed schema evolution mode");
            }
            try {
                final Schema schema = confluentSchemaRegistryClient.getByID(schemaId);
                if (schema != null) {
                    configBuilder.withFixedSchema(schema);
                } else {
                    logger.error("Required schema not found with ID {} for datastream with name '{}'", schemaId, datastreamName);
                    throw new IllegalStateException("required schema not found for datastream");
                }

            } catch (IOException | RestClientException e) {
                logger.error("Error fetching schema with ID {} for datastream with name '{}'", schemaId, datastreamName, e);
                throw new DatastreamRuntimeException("Error fetching schema for datastream", e);
            }
        } else if (schemaId != null) {
            logger.warn("Schema ID provided for datastream with name '{}', but schema evolution mode is not set to fixed. Ignoring...", datastreamName);
        }

        Optional.ofNullable(partitionExpirationDays).ifPresent(configBuilder::withPartitionExpirationDays);

        Optional.ofNullable(deadLetterTableConfiguration).ifPresent(configBuilder::withDeadLetterTableConfiguration);

        if (labels != null && !labels.isEmpty()) {
            configBuilder.withLabels(labels);
        }

        return configBuilder.build();
    }

}
