/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.bigquery;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

import com.linkedin.datastream.bigquery.schema.BigquerySchemaEvolver;
import com.linkedin.datastream.bigquery.schema.BigquerySchemaEvolverFactory;
import com.linkedin.datastream.bigquery.schema.BigquerySchemaEvolverType;
import com.linkedin.datastream.common.VerifiableProperties;
import com.linkedin.datastream.serde.Deserializer;
import com.linkedin.datastream.server.Pair;
import com.linkedin.datastream.server.api.transport.TransportProviderAdmin;
import com.linkedin.datastream.server.api.transport.TransportProviderAdminFactory;


import static com.linkedin.datastream.bigquery.BigqueryTransportProviderAdmin.METADATA_DATASET_KEY;
import static com.linkedin.datastream.bigquery.BigqueryTransportProviderAdmin.METADATA_EXCEPTIONS_TABLE_ENABLED_KEY;
import static com.linkedin.datastream.bigquery.BigqueryTransportProviderAdmin.METADATA_MANAGE_DESTINATION_TABLE_KEY;
import static com.linkedin.datastream.bigquery.BigqueryTransportProviderAdmin.METADATA_PARTITION_EXPIRATION_DAYS_KEY;
import static com.linkedin.datastream.bigquery.BigqueryTransportProviderAdmin.METADATA_PROJECT_ID_KEY;
import static com.linkedin.datastream.bigquery.BigqueryTransportProviderAdmin.METADATA_SCHEMA_EVOLVER_KEY;
import static com.linkedin.datastream.bigquery.BigqueryTransportProviderAdmin.METADATA_TABLE_NAME_TEMPLATE_KEY;
import static com.linkedin.datastream.bigquery.BigqueryTransportProviderAdmin.METADATA_TABLE_SUFFIX_KEY;

/**
 * Simple Bigquery Transport provider factory that creates one producer for the entire system
 */
public class BigqueryTransportProviderAdminFactory implements TransportProviderAdminFactory {

    private final Logger log = LoggerFactory.getLogger(BigqueryTransportProviderAdminFactory.class);

    private static final String CONFIG_BATCHBUILDER_QUEUE_SIZE = "batchBuilderQueueSize";
    private static final String CONFIG_BATCHBUILDER_THREAD_COUNT = "batchBuilderThreadCount";
    private static final String CONFIG_MAX_BATCH_SIZE = "maxBatchSize";
    private static final String CONFIG_MAX_BATCH_AGE = "maxBatchAge";
    private static final String CONFIG_MAX_INFLIGHT_COMMITS = "maxInflightCommits";
    private static final String CONFIG_SCHEMA_REGISTRY_URL = "translator.schemaRegistry.URL";

    private static final String CONFIG_COMMITTER_DOMAIN_PREFIX = "committer";
    private static final String CONFIG_COMMITTER_THREADS = "threads";

    private static final String CONFIG_SCHEMA_EVOLVERS_DOMAIN_PREFIX = "schemaEvolvers";

    private static final String CONFIG_DEFAULT_METADATA_PREFIX = "defaultMetadata";

    @Override
    public TransportProviderAdmin createTransportProviderAdmin(String transportProviderName,
                                                               Properties transportProviderProperties) {
        final VerifiableProperties tpProperties = new VerifiableProperties(transportProviderProperties);

        final Map<BigqueryDatastreamDestination, BigqueryDatastreamConfiguration> datastreamConfigByDestination = new ConcurrentHashMap<>();
        final String committerProjectId;
        final BigqueryBatchCommitter committer;
        {
            final VerifiableProperties committerProperties = new VerifiableProperties(tpProperties.getDomainProperties(CONFIG_COMMITTER_DOMAIN_PREFIX));
            final BigQuery bigQuery = constructBigQueryClientFromProperties(committerProperties);
            committerProjectId = committerProperties.getString("projectId");
            final int committerThreads = committerProperties.getInt(CONFIG_COMMITTER_THREADS, 1);
            committer = new BigqueryBatchCommitter(bigQuery, committerThreads, datastreamConfigByDestination);
        }

        final int maxBatchAge = tpProperties.getInt(CONFIG_MAX_BATCH_AGE, 500);
        final List<BatchBuilder> batchBuilders;
        final Serializer valueSerializer;
        final Deserializer valueDeserializer;
        final Map<String, String> defaultMetadata;
        final Map<String, BigquerySchemaEvolver> bigquerySchemaEvolverMap;
        final BigquerySchemaEvolver defaultSchemaEvolver;
        {
            {
                final io.confluent.kafka.schemaregistry.client.SchemaRegistryClient confluentSchemaRegistryClient = new CachedSchemaRegistryClient(
                        tpProperties.getStringList(CONFIG_SCHEMA_REGISTRY_URL), AbstractKafkaAvroSerDeConfig.MAX_SCHEMAS_PER_SUBJECT_DEFAULT);

                final Map<String, Object> valueSerDeConfig = new HashMap<>();
                valueSerDeConfig.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, tpProperties.getString(CONFIG_SCHEMA_REGISTRY_URL));

                valueSerializer = new KafkaSerializer(new KafkaAvroSerializer(confluentSchemaRegistryClient, valueSerDeConfig));
                valueDeserializer = new KafkaDeserializer(new KafkaAvroDeserializer(confluentSchemaRegistryClient, valueSerDeConfig));
            }

            {
                final VerifiableProperties defaultMetadataProperties = new VerifiableProperties(tpProperties
                        .getDomainProperties(CONFIG_DEFAULT_METADATA_PREFIX));
                defaultMetadata = Stream.of(METADATA_PROJECT_ID_KEY, METADATA_DATASET_KEY, METADATA_TABLE_NAME_TEMPLATE_KEY,
                        METADATA_TABLE_SUFFIX_KEY, METADATA_PARTITION_EXPIRATION_DAYS_KEY, METADATA_SCHEMA_EVOLVER_KEY,
                        METADATA_EXCEPTIONS_TABLE_ENABLED_KEY, METADATA_MANAGE_DESTINATION_TABLE_KEY)
                        .flatMap(key -> Optional.ofNullable(defaultMetadataProperties.getProperty(key))
                                .map(value -> Stream.of(Pair.of(key, value))).orElse(null))
                        .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
                defaultMetadata.putIfAbsent(METADATA_PROJECT_ID_KEY, committerProjectId);
            }

            {
                final VerifiableProperties schemaEvolversProperties = new VerifiableProperties(tpProperties
                        .getDomainProperties(CONFIG_SCHEMA_EVOLVERS_DOMAIN_PREFIX));
                bigquerySchemaEvolverMap = Collections.unmodifiableMap(BigquerySchemaEvolverFactory.createBigquerySchemaEvolvers(schemaEvolversProperties));
                defaultSchemaEvolver = Optional.ofNullable(defaultMetadata.get(METADATA_SCHEMA_EVOLVER_KEY)).map(bigquerySchemaEvolverMap::get)
                        .orElseGet(() -> BigquerySchemaEvolverFactory.createBigquerySchemaEvolver(BigquerySchemaEvolverType.noop));
            }

            final int maxBatchSize = tpProperties.getInt(CONFIG_MAX_BATCH_SIZE, 100000);
            final int maxInFlightCommits = tpProperties.getInt(CONFIG_MAX_INFLIGHT_COMMITS, 1);
            final int batchQueueSize = tpProperties.getInt(CONFIG_BATCHBUILDER_QUEUE_SIZE, 1000);
            final int batchBuilderCount = tpProperties.getInt(CONFIG_BATCHBUILDER_THREAD_COUNT, 5);
            batchBuilders = IntStream.range(0, batchBuilderCount)
                    .mapToObj(i -> new BatchBuilder(
                            maxBatchSize,
                            maxBatchAge,
                            maxInFlightCommits,
                            committer,
                            batchQueueSize,
                            valueDeserializer,
                            datastreamConfigByDestination)).collect(Collectors.toList());
        }

        final BigqueryBufferedTransportProvider bufferedTransportProvider = new BigqueryBufferedTransportProvider(transportProviderName, batchBuilders,
                committer, maxBatchAge);
        return new BigqueryTransportProviderAdmin(bufferedTransportProvider, valueSerializer, valueDeserializer, defaultMetadata,
                defaultSchemaEvolver, datastreamConfigByDestination, bigquerySchemaEvolverMap, new BigqueryTransportProviderFactory());
    }

    private BigQuery constructBigQueryClientFromProperties(final VerifiableProperties properties) {
        String credentialsPath = properties.getString("credentialsPath");
        String projectId = properties.getString("projectId");
        try {
            Credentials credentials = GoogleCredentials
                    .fromStream(new FileInputStream(credentialsPath));
            return BigQueryOptions.newBuilder()
                    .setProjectId(projectId)
                    .setCredentials(credentials).build().getService();
        } catch (FileNotFoundException e) {
            log.error("Credentials path {} does not exist", credentialsPath);
            throw new RuntimeException(e);
        } catch (IOException e) {
            log.error("Unable to read credentials: {}", credentialsPath);
            throw new RuntimeException(e);
        }
    }

}
