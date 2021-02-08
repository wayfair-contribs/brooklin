/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.bigquery;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;

import com.linkedin.datastream.bigquery.schema.BigquerySchemaEvolver;
import com.linkedin.datastream.bigquery.schema.BigquerySchemaEvolverFactory;
import com.linkedin.datastream.bigquery.schema.BigquerySchemaEvolverType;
import com.linkedin.datastream.common.VerifiableProperties;
import com.linkedin.datastream.server.api.transport.TransportProviderAdmin;
import com.linkedin.datastream.server.api.transport.TransportProviderAdminFactory;

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
    private static final String CONFIG_COMMITTER_PROJECT_ID = "projectId";
    private static final String CONFIG_COMMITTER_CREDENTIALS_PATH = "credentialsPath";

    @Override
    public TransportProviderAdmin createTransportProviderAdmin(String transportProviderName,
                                                               Properties transportProviderProperties) {
        final VerifiableProperties tpProperties = new VerifiableProperties(transportProviderProperties);

        // Create a map to define datastream destination to configuration mappings and share with multiple components.
        // Datastream destination to configuration mappings are initialized when new datastreams are created or assigned for processing.
        // TODO: potentially replace the shared map with a datastream configuration registry implementation
        final Map<BigqueryDatastreamDestination, BigqueryDatastreamConfiguration> datastreamConfigByDestination = new ConcurrentHashMap<>();
        final String committerProjectId;
        final BigqueryBatchCommitter committer;
        {
            final VerifiableProperties committerProperties = new VerifiableProperties(tpProperties.getDomainProperties(CONFIG_COMMITTER_DOMAIN_PREFIX));
            committerProjectId = committerProperties.getString(CONFIG_COMMITTER_PROJECT_ID);
            final String committerCredentialsPath = committerProperties.getString(CONFIG_COMMITTER_CREDENTIALS_PATH);
            final BigQuery bigQuery = constructBigQueryClient(committerProjectId, committerCredentialsPath);
            final int committerThreads = committerProperties.getInt(CONFIG_COMMITTER_THREADS, 1);
            committer = new BigqueryBatchCommitter(bigQuery, committerThreads, datastreamConfigByDestination);
        }

        final String legacyDefaultSchemaRegistryUrl = tpProperties.getProperty(CONFIG_SCHEMA_REGISTRY_URL);

        final int maxBatchAge = tpProperties.getInt(CONFIG_MAX_BATCH_AGE, 500);
        final List<BatchBuilder> batchBuilders;
        final Map<String, BigquerySchemaEvolver> bigquerySchemaEvolverMap;
        final BigquerySchemaEvolver defaultSchemaEvolver;
        {
            bigquerySchemaEvolverMap = Arrays.stream(BigquerySchemaEvolverType.values())
                    .collect(Collectors.toMap(BigquerySchemaEvolverType::getModeName, BigquerySchemaEvolverFactory::createBigquerySchemaEvolver));
            defaultSchemaEvolver = BigquerySchemaEvolverFactory.createBigquerySchemaEvolver(BigquerySchemaEvolverType.dynamic);

            // Adjusted default max batch size to 500 based on Google's recommendation: https://cloud.google.com/bigquery/quotas#streaming_inserts
            final int maxBatchSize = tpProperties.getInt(CONFIG_MAX_BATCH_SIZE, 500);
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
                            datastreamConfigByDestination)).collect(Collectors.toList());
        }

        final BigqueryBufferedTransportProvider bufferedTransportProvider = new BigqueryBufferedTransportProvider(transportProviderName, batchBuilders,
                committer, maxBatchAge);
        return new BigqueryTransportProviderAdmin(bufferedTransportProvider,
                defaultSchemaEvolver, datastreamConfigByDestination, bigquerySchemaEvolverMap, new BigqueryTransportProviderFactory(),
                new BigqueryDatastreamConfigurationFactory(),
                committerProjectId,
                legacyDefaultSchemaRegistryUrl);
    }

    private BigQuery constructBigQueryClient(final String projectId, final String credentialsPath) {
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
