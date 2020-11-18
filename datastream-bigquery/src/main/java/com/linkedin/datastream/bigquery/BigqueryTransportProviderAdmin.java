/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.bigquery;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.linkedin.datastream.serde.Deserializer;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.bigquery.schema.BigquerySchemaEvolver;
import com.linkedin.datastream.bigquery.schema.BigquerySchemaEvolverFactory;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.common.VerifiableProperties;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.api.connector.DatastreamValidationException;
import com.linkedin.datastream.server.api.transport.TransportProvider;
import com.linkedin.datastream.server.api.transport.TransportProviderAdmin;

/**
 * {@link TransportProviderAdmin} implementation for {@link BigqueryTransportProvider}
 *
 * <ul>
 *  <li>Initializes {@link BigqueryTransportProvider}</li>
 *  <li>Sets up the correct destination connection string/bq table</li>
 * </ul>
 */
public class BigqueryTransportProviderAdmin implements TransportProviderAdmin {
    private static final Logger LOG = LoggerFactory.getLogger(BigqueryTransportProviderAdmin.class);

    private static final String CONFIG_BATCHBUILDER_QUEUE_SIZE = "batchBuilderQueueSize";
    private static final String CONFIG_BATCHBUILDER_THREAD_COUNT = "batchBuilderThreadCount";
    private static final String CONFIG_MAX_BATCH_SIZE = "maxBatchSize";
    private static final String CONFIG_MAX_BATCH_AGE = "maxBatchAge";
    private static final String CONFIG_MAX_INFLIGHT_COMMITS = "maxInflightCommits";

    private static final String CONFIG_SCHEMA_REGISTRY_URL = "translator.schemaRegistry.URL";

    protected static final String CONFIG_COMMITTER_DOMAIN_PREFIX = "committer";
    private static final String CONFIG_THREADS = "threads";

    protected static final String CONFIG_SCHEMA_EVOLVERS_DOMAIN_PREFIX = "schemaEvolvers";

    private final BigqueryTransportProvider _transportProvider;

    /**
     * Constructor for BigqueryTransportProviderAdmin.
     * @param props TransportProviderAdmin configuration properties, e.g. number of committer threads, file format.
     */
    public BigqueryTransportProviderAdmin(final String transportProviderName, final Properties props) {
        final VerifiableProperties tpProperties = new VerifiableProperties(props);

        final BigqueryBatchCommitter committer;
        {
            final VerifiableProperties committerProperties = new VerifiableProperties(tpProperties.getDomainProperties(CONFIG_COMMITTER_DOMAIN_PREFIX));
            final BigQuery bigQuery = constructBigQueryClientFromProperties(committerProperties);
            final int committerThreads = committerProperties.getInt(CONFIG_THREADS, 1);
            committer = new BigqueryBatchCommitter(bigQuery, committerThreads);
        }

        final int maxBatchAge = tpProperties.getInt(CONFIG_MAX_BATCH_AGE, 500);
        final List<BatchBuilder> batchBuilders;
        final Serializer valueSerializer;
        final Deserializer valueDeserializer;
        {
            {
                final io.confluent.kafka.schemaregistry.client.SchemaRegistryClient confluentSchemaRegistryClient = new CachedSchemaRegistryClient(
                        tpProperties.getStringList(CONFIG_SCHEMA_REGISTRY_URL), AbstractKafkaAvroSerDeConfig.MAX_SCHEMAS_PER_SUBJECT_DEFAULT);

                final Map<String, Object> valueSerDeConfig = new HashMap<>();
                valueSerDeConfig.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, tpProperties.getString(CONFIG_SCHEMA_REGISTRY_URL));

                valueSerializer = new KafkaSerializer(new KafkaAvroSerializer(confluentSchemaRegistryClient, valueSerDeConfig));
                valueDeserializer = new KafkaDeserializer(new KafkaAvroDeserializer(confluentSchemaRegistryClient, valueSerDeConfig));
            }

            final VerifiableProperties schemaEvolversProperties = new VerifiableProperties(tpProperties.getDomainProperties(CONFIG_SCHEMA_EVOLVERS_DOMAIN_PREFIX));
            final String defaultSchemaEvolverName = schemaEvolversProperties.getString("default");
            final Map<String, BigquerySchemaEvolver> bigquerySchemaEvolverMap = BigquerySchemaEvolverFactory.createBigquerySchemaEvolvers(schemaEvolversProperties);

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
                            bigquerySchemaEvolverMap,
                            defaultSchemaEvolverName)).collect(Collectors.toList());
        }

        _transportProvider = new BigqueryTransportProvider.BigqueryTransportProviderBuilder()
                .setTransportProviderName(transportProviderName)
                .setMaxBatchAge(maxBatchAge)
                .setCommitter(committer)
                .setValueSerializer(valueSerializer)
                .setValueDeserializer(valueDeserializer)
                .setBatchBuilders(batchBuilders)
                .build();
    }


    private static BigQuery constructBigQueryClientFromProperties(final VerifiableProperties properties) {
        String credentialsPath = properties.getString("credentialsPath");
        String projectId = properties.getString("projectId");
        try {
            Credentials credentials = GoogleCredentials
                    .fromStream(new FileInputStream(credentialsPath));
            return BigQueryOptions.newBuilder()
                    .setProjectId(projectId)
                    .setCredentials(credentials).build().getService();
        } catch (FileNotFoundException e) {
            LOG.error("Credentials path {} does not exist", credentialsPath);
            throw new RuntimeException(e);
        } catch (IOException e) {
            LOG.error("Unable to read credentials: {}", credentialsPath);
            throw new RuntimeException(e);
        }
    }

    @Override
    public TransportProvider assignTransportProvider(DatastreamTask task) {
        return _transportProvider;
    }

    @Override
    public void unassignTransportProvider(DatastreamTask task) {
    }

    @Override
    public void initializeDestinationForDatastream(Datastream datastream, String destinationName)
            throws DatastreamValidationException {
        if (!datastream.hasDestination()) {
            datastream.setDestination(new DatastreamDestination());
        }

        if (!datastream.getMetadata().containsKey("dataset")) {
            throw new DatastreamValidationException("Metadata dataset is not set in the datastream definition.");
        }

        String destination = datastream.getMetadata().get("dataset")
                + "/"
                + (datastream.getMetadata().containsKey("partitionExpirationDays") ? datastream.getMetadata().get("partitionExpirationDays") : "-1")
                + "/"
                + (datastream.getMetadata().containsKey("tableSuffix") ? datastream.getMetadata().get("tableSuffix") : "");

        datastream.getDestination().setConnectionString(destination);
    }

    @Override
    public void createDestination(Datastream datastream) {
    }

    @Override
    public void dropDestination(Datastream datastream) {
    }

    @Override
    public Duration getRetention(Datastream datastream) {
        return Duration.ofSeconds(0);
    }
}
