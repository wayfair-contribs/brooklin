/*
 * Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */

package com.linkedin.datastream.bigquery;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.avro.Schema;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import com.linkedin.data.template.StringMap;
import com.linkedin.datastream.bigquery.schema.BigquerySchemaEvolver;
import com.linkedin.datastream.bigquery.schema.BigquerySchemaEvolverFactory;
import com.linkedin.datastream.bigquery.schema.BigquerySchemaEvolverType;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.serde.Deserializer;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.DatastreamTaskImpl;
import com.linkedin.datastream.server.api.transport.TransportProvider;
import com.linkedin.datastream.testutil.DatastreamTestUtils;

import static com.linkedin.datastream.bigquery.BigqueryTransportProviderAdmin.METADATA_AUTO_CREATE_TABLE_KEY;
import static com.linkedin.datastream.bigquery.BigqueryTransportProviderAdmin.METADATA_DEAD_LETTER_TABLE_KEY;
import static com.linkedin.datastream.bigquery.BigqueryTransportProviderAdmin.METADATA_LABELS_KEY;
import static com.linkedin.datastream.bigquery.BigqueryTransportProviderAdmin.METADATA_PARTITION_EXPIRATION_DAYS_KEY;
import static com.linkedin.datastream.bigquery.BigqueryTransportProviderAdmin.METADATA_SCHEMA_EVOLUTION_MODE_KEY;
import static com.linkedin.datastream.bigquery.BigqueryTransportProviderAdmin.METADATA_SCHEMA_ID_KEY;
import static com.linkedin.datastream.bigquery.BigqueryTransportProviderAdmin.METADATA_SCHEMA_REGISTRY_LOCATION_KEY;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Tests for BigqueryTransportProviderAdmin.
 */
public class BigqueryTransportProviderAdminTests {

    private BigqueryBufferedTransportProvider bufferedTransportProvider;
    private Serializer serializer;
    private Deserializer deserializer;
    private String defaultProjectId;
    private String defaultSchemaRegistryUrl;
    private BigquerySchemaEvolver defaultSchemaEvolver;
    private ConcurrentMap<BigqueryDatastreamDestination, BigqueryDatastreamConfiguration> datastreamConfigByDestination;
    private Map<String, BigquerySchemaEvolver> bigquerySchemaEvolverMap;
    private BigqueryTransportProviderFactory bigqueryTransportProviderFactory;
    private BigqueryDatastreamConfigurationFactory bigqueryDatastreamConfigurationFactory;

    @BeforeMethod
    public void beforeTest() {
        bufferedTransportProvider = mock(BigqueryBufferedTransportProvider.class);
        serializer = mock(Serializer.class);
        deserializer = mock(Deserializer.class);
        defaultProjectId = "projectId";
        defaultSchemaRegistryUrl = "https://schema-registry";
        defaultSchemaEvolver = BigquerySchemaEvolverFactory.createBigquerySchemaEvolver(BigquerySchemaEvolverType.dynamic);
        datastreamConfigByDestination = new ConcurrentHashMap<>();
        bigquerySchemaEvolverMap = Arrays.stream(BigquerySchemaEvolverType.values())
                .collect(Collectors.toMap(BigquerySchemaEvolverType::getModeName, BigquerySchemaEvolverFactory::createBigquerySchemaEvolver));
        bigqueryTransportProviderFactory = mock(BigqueryTransportProviderFactory.class);
        bigqueryDatastreamConfigurationFactory = mock(BigqueryDatastreamConfigurationFactory.class);
    }

    @Test
    public void testAssignTask() {
        final BigqueryTransportProviderAdmin admin = new BigqueryTransportProviderAdmin(
                bufferedTransportProvider,
                defaultSchemaEvolver,
                datastreamConfigByDestination,
                bigquerySchemaEvolverMap,
                bigqueryTransportProviderFactory,
                bigqueryDatastreamConfigurationFactory,
                defaultProjectId,
                defaultSchemaRegistryUrl
        );
        final String datastreamName = "test";
        final String schemaRegistryLocation = "https://schema-registry";
        final BigqueryDatastreamDestination destination = new BigqueryDatastreamDestination("project", "dataset", "table");
        final Datastream datastream = DatastreamTestUtils.createDatastream("connector", datastreamName, "source", destination.toString(), 1);
        datastream.getMetadata().put(METADATA_SCHEMA_REGISTRY_LOCATION_KEY, schemaRegistryLocation);
        final DatastreamTask task = new DatastreamTaskImpl(Collections.singletonList(datastream));
        final BigqueryDatastreamConfiguration config = BigqueryDatastreamConfiguration.builder(
                destination,
                defaultSchemaEvolver,
                true,
                deserializer,
                serializer
        ).build();
        when(bigqueryDatastreamConfigurationFactory.createBigqueryDatastreamConfiguration(
                destination,
                datastreamName,
                schemaRegistryLocation,
                config.getSchemaEvolver(),
                config.isCreateDestinationTableEnabled(),
                null,
                null,
                null,
                null
        )).thenReturn(config);
        final BigqueryTransportProvider bigqueryTransportProvider = mock(BigqueryTransportProvider.class);

        when(bigqueryTransportProviderFactory.createTransportProvider(bufferedTransportProvider, serializer, deserializer, config, datastreamConfigByDestination
        )).thenReturn(bigqueryTransportProvider);

        final TransportProvider transportProvider = admin.assignTransportProvider(task);

        assertEquals(transportProvider, bigqueryTransportProvider);
        assertEquals(admin.getDatastreamTransportProviders().get(datastream), transportProvider);
        assertEquals(admin.getTransportProviderTasks().get(transportProvider), Collections.singleton(task));
    }

    @Test
    public void testUnassignTask() {
        final BigqueryTransportProviderAdmin admin = new BigqueryTransportProviderAdmin(
                bufferedTransportProvider,
                defaultSchemaEvolver,
                datastreamConfigByDestination,
                bigquerySchemaEvolverMap,
                bigqueryTransportProviderFactory,
                bigqueryDatastreamConfigurationFactory,
                defaultProjectId,
                defaultSchemaRegistryUrl
        );
        final String datastreamName = "test";
        final String schemaRegistryLocation = "https://schema-registry";
        final BigqueryDatastreamDestination destination = new BigqueryDatastreamDestination("project", "dataset", "table");
        final Datastream datastream = DatastreamTestUtils.createDatastream("connector", datastreamName, "source", destination.toString(), 1);
        datastream.getMetadata().put(METADATA_SCHEMA_REGISTRY_LOCATION_KEY, schemaRegistryLocation);
        final DatastreamTask task = new DatastreamTaskImpl(Collections.singletonList(datastream));
        final BigqueryDatastreamConfiguration config = BigqueryDatastreamConfiguration.builder(
                destination,
                defaultSchemaEvolver,
                true,
                deserializer,
                serializer
        ).build();
        when(bigqueryDatastreamConfigurationFactory.createBigqueryDatastreamConfiguration(
                destination,
                datastreamName,
                schemaRegistryLocation,
                config.getSchemaEvolver(),
                config.isCreateDestinationTableEnabled(),
                null,
                null,
                null,
                null
        )).thenReturn(config);
        final BigqueryTransportProvider bigqueryTransportProvider = mock(BigqueryTransportProvider.class);

        when(bigqueryTransportProviderFactory.createTransportProvider(bufferedTransportProvider, serializer, deserializer, config, datastreamConfigByDestination
        )).thenReturn(bigqueryTransportProvider);

        final TransportProvider transportProvider = admin.assignTransportProvider(task);
        datastreamConfigByDestination.put(destination, config);

        assertEquals(transportProvider, bigqueryTransportProvider);
        assertEquals(admin.getDatastreamTransportProviders().get(datastream), transportProvider);
        assertEquals(admin.getTransportProviderTasks().get(transportProvider), Collections.singleton(task));

        admin.unassignTransportProvider(task);

        assertFalse(admin.getDatastreamTransportProviders().containsKey(datastream));
        assertFalse(admin.getTransportProviderTasks().containsKey(transportProvider));
        verify(bigqueryTransportProvider).close();
        assertEquals(datastreamConfigByDestination.size(), 1);
    }

    @Test
    public void testParallelAssignAndUnassign() {
        final BigqueryTransportProviderAdmin admin = new BigqueryTransportProviderAdmin(
                bufferedTransportProvider,
                defaultSchemaEvolver,
                datastreamConfigByDestination,
                bigquerySchemaEvolverMap,
                bigqueryTransportProviderFactory,
                bigqueryDatastreamConfigurationFactory,
                defaultProjectId,
                defaultSchemaRegistryUrl
        );
        final String datastreamName = "test";
        final String schemaRegistryLocation = "https://schema-registry";
        final BigqueryDatastreamDestination destination = new BigqueryDatastreamDestination("project", "dataset", "table");
        final Datastream datastream = DatastreamTestUtils.createDatastream("connector", datastreamName, "source", destination.toString(), 1);
        datastream.getMetadata().put(METADATA_SCHEMA_REGISTRY_LOCATION_KEY, schemaRegistryLocation);
        final BigqueryDatastreamConfiguration config = BigqueryDatastreamConfiguration.builder(
                destination,
                defaultSchemaEvolver,
                true,
                deserializer,
                serializer
        ).build();
        when(bigqueryDatastreamConfigurationFactory.createBigqueryDatastreamConfiguration(
                destination,
                datastreamName,
                schemaRegistryLocation,
                config.getSchemaEvolver(),
                config.isCreateDestinationTableEnabled(),
                null,
                null,
                null,
                null
        )).thenReturn(config);
        final BigqueryTransportProvider bigqueryTransportProvider = mock(BigqueryTransportProvider.class);

        when(bigqueryTransportProviderFactory.createTransportProvider(bufferedTransportProvider, serializer, deserializer, config, datastreamConfigByDestination
        )).thenReturn(bigqueryTransportProvider);
        when(bigqueryTransportProvider.getDestinations()).thenReturn(ImmutableSet.of(destination));

        final DatastreamTask task = new DatastreamTaskImpl(Collections.singletonList(datastream));

        admin.assignTransportProvider(task);

        final ExecutorService executor = Executors.newCachedThreadPool();

        final List<Future<?>> results = IntStream.range(0, 100).boxed().map(i -> executor.submit(() -> {
            admin.unassignTransportProvider(task);
            admin.assignTransportProvider(task);
            datastreamConfigByDestination.put(destination, config);
        })).collect(Collectors.toList());

        results.forEach(r -> {
            try {
                r.get();
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        });

        assertEquals(datastreamConfigByDestination.size(), 1);
    }


    @Test
    public void testAssignMultipleTasksPerDatastream() {
        final BigqueryTransportProviderAdmin admin = new BigqueryTransportProviderAdmin(
                bufferedTransportProvider,
                defaultSchemaEvolver,
                datastreamConfigByDestination,
                bigquerySchemaEvolverMap,
                bigqueryTransportProviderFactory,
                bigqueryDatastreamConfigurationFactory,
                defaultProjectId,
                defaultSchemaRegistryUrl
        );
        final String datastreamName = "test";
        final String schemaRegistryLocation = "https://schema-registry";
        final BigqueryDatastreamDestination destination = new BigqueryDatastreamDestination("project", "dataset", "table");
        final Datastream datastream = DatastreamTestUtils.createDatastream("connector", datastreamName, "source", destination.toString(), 1);
        datastream.getMetadata().put(METADATA_SCHEMA_REGISTRY_LOCATION_KEY, schemaRegistryLocation);
        final BigqueryDatastreamConfiguration config = BigqueryDatastreamConfiguration.builder(
                destination,
                defaultSchemaEvolver,
                true,
                deserializer,
                serializer
        ).build();
        when(bigqueryDatastreamConfigurationFactory.createBigqueryDatastreamConfiguration(
                destination,
                datastreamName,
                schemaRegistryLocation,
                config.getSchemaEvolver(),
                config.isCreateDestinationTableEnabled(),
                null,
                null,
                null,
                null
        )).thenReturn(config);
        final BigqueryTransportProvider bigqueryTransportProvider = mock(BigqueryTransportProvider.class);

        when(bigqueryTransportProviderFactory.createTransportProvider(bufferedTransportProvider, serializer, deserializer, config, datastreamConfigByDestination
        )).thenReturn(bigqueryTransportProvider);
        when(bigqueryTransportProvider.getDestinations()).thenReturn(ImmutableSet.of(destination));

        final DatastreamTask firstTask = new DatastreamTaskImpl(Collections.singletonList(datastream));

        admin.assignTransportProvider(firstTask);
        datastreamConfigByDestination.put(destination, config);

        final DatastreamTask secondTask = new DatastreamTaskImpl(Collections.singletonList(datastream));
        admin.assignTransportProvider(secondTask);

        admin.unassignTransportProvider(firstTask);

        assertEquals(datastreamConfigByDestination.size(), 1);
        verify(bigqueryTransportProvider, never()).close();
    }

    @DataProvider(name = "datastream config test cases")
    public Object[][] datastreamConfigTestCases() {
        try {
            return new Object[][]{
                {
                        new BigqueryDatastreamDestination("project", "dataset", "dest"),
                        ImmutableMap.of(
                                METADATA_SCHEMA_REGISTRY_LOCATION_KEY, "https://schema-registry",
                                METADATA_SCHEMA_ID_KEY, "1",
                                METADATA_SCHEMA_EVOLUTION_MODE_KEY, BigquerySchemaEvolverType.fixed.getModeName()
                        ),
                        BigqueryDatastreamConfiguration.builder(new BigqueryDatastreamDestination("project", "dataset", "dest"),
                                BigquerySchemaEvolverFactory.createBigquerySchemaEvolver(BigquerySchemaEvolverType.fixed), true, deserializer, serializer)
                                .withFixedSchema(mock(Schema.class))
                                .withDeadLetterTableConfiguration(BigqueryDatastreamConfiguration
                                        .builder(new BigqueryDatastreamDestination("project", "dataset", "dest_exceptions"),
                                                BigquerySchemaEvolverFactory.createBigquerySchemaEvolver(BigquerySchemaEvolverType.dynamic),
                                                true,
                                                deserializer, serializer).build())
                                .build()
                },
                {
                        new BigqueryDatastreamDestination("project", "dataset", "dest"),
                        ImmutableMap.of(
                                METADATA_SCHEMA_REGISTRY_LOCATION_KEY, "https://schema-registry",
                                METADATA_SCHEMA_EVOLUTION_MODE_KEY, BigquerySchemaEvolverType.dynamic.getModeName()
                        ),
                        BigqueryDatastreamConfiguration.builder(new BigqueryDatastreamDestination("project", "dataset", "dest"),
                                BigquerySchemaEvolverFactory.createBigquerySchemaEvolver(BigquerySchemaEvolverType.dynamic), true, deserializer, serializer)
                                .withDeadLetterTableConfiguration(BigqueryDatastreamConfiguration
                                        .builder(new BigqueryDatastreamDestination("project", "dataset", "dest_exceptions"),
                                                BigquerySchemaEvolverFactory.createBigquerySchemaEvolver(BigquerySchemaEvolverType.dynamic),
                                                true,
                                                deserializer, serializer).build())
                                .build()
                },
                {
                        new BigqueryDatastreamDestination("project", "dataset", "dest"),
                        ImmutableMap.of(
                            METADATA_SCHEMA_REGISTRY_LOCATION_KEY, "https://schema-registry",
                            METADATA_SCHEMA_EVOLUTION_MODE_KEY, BigquerySchemaEvolverType.dynamic.getModeName(),
                            METADATA_LABELS_KEY, "test,name:value"
                        ),
                        BigqueryDatastreamConfiguration.builder(new BigqueryDatastreamDestination("project", "dataset", "dest"),
                                BigquerySchemaEvolverFactory.createBigquerySchemaEvolver(BigquerySchemaEvolverType.dynamic), true, deserializer, serializer)
                                .withDeadLetterTableConfiguration(BigqueryDatastreamConfiguration
                                        .builder(new BigqueryDatastreamDestination("project", "dataset", "dest_exceptions"),
                                                BigquerySchemaEvolverFactory.createBigquerySchemaEvolver(BigquerySchemaEvolverType.dynamic),
                                                true,
                                                deserializer, serializer).build())
                                .withLabels(ImmutableList.of(BigqueryLabel.of("test"), BigqueryLabel.of("name", "value"))).build()
                },
                {
                        new BigqueryDatastreamDestination("project", "dataset", "dest"),
                        ImmutableMap.of(
                                METADATA_SCHEMA_REGISTRY_LOCATION_KEY, "https://schema-registry",
                                METADATA_SCHEMA_EVOLUTION_MODE_KEY, BigquerySchemaEvolverType.dynamic.getModeName(),
                                METADATA_DEAD_LETTER_TABLE_KEY, new BigqueryDatastreamDestination("project", "dataset", "deadLetterTable").toString()
                        ),
                        BigqueryDatastreamConfiguration.builder(new BigqueryDatastreamDestination("project", "dataset", "dest"),
                                BigquerySchemaEvolverFactory.createBigquerySchemaEvolver(BigquerySchemaEvolverType.dynamic), true, deserializer, serializer)
                                .withDeadLetterTableConfiguration(BigqueryDatastreamConfiguration.builder(
                                        new BigqueryDatastreamDestination("project", "dataset", "deadLetterTable"),
                                        BigquerySchemaEvolverFactory.createBigquerySchemaEvolver(BigquerySchemaEvolverType.dynamic), true, deserializer, serializer).build()).build()
                }
            };
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Test(dataProvider = "datastream config test cases")
    public void testGetConfigurationFromDatastream(
        final BigqueryDatastreamDestination destination,
        final Map<String, String> datastreamMetadata,
        final BigqueryDatastreamConfiguration expectedConfiguration
    ) {
        final BigqueryTransportProviderAdmin admin = new BigqueryTransportProviderAdmin(
                bufferedTransportProvider,
                defaultSchemaEvolver,
                datastreamConfigByDestination,
                bigquerySchemaEvolverMap,
                bigqueryTransportProviderFactory,
                bigqueryDatastreamConfigurationFactory,
                defaultProjectId,
                defaultSchemaRegistryUrl
        );
        final String datastreamName = "test";
        final Datastream datastream = DatastreamTestUtils.createDatastream("connector", datastreamName, "source", destination.toString(), 1);
        datastream.setMetadata(new StringMap(datastreamMetadata));
        final BigqueryDatastreamDestination deadLetterTable = BigqueryDatastreamDestination.parse(Optional.ofNullable(datastreamMetadata.get(METADATA_DEAD_LETTER_TABLE_KEY))
                .orElseGet(() -> new BigqueryDatastreamDestination(destination.getProjectId(), destination.getDatasetId(), destination.getDestinatonName() + "_exceptions").toString()));
        final String schemaRegistryUrl = datastreamMetadata.getOrDefault(METADATA_SCHEMA_REGISTRY_LOCATION_KEY, defaultSchemaRegistryUrl);
        final BigquerySchemaEvolver schemaEvolver = bigquerySchemaEvolverMap.getOrDefault(datastreamMetadata.getOrDefault(METADATA_SCHEMA_EVOLUTION_MODE_KEY, ""), defaultSchemaEvolver);
        final boolean autoCreateTable = Boolean.parseBoolean(datastreamMetadata.getOrDefault(METADATA_AUTO_CREATE_TABLE_KEY, Boolean.TRUE.toString()));
        final Long partitionExpirationDays = Optional.ofNullable(datastreamMetadata.get(METADATA_PARTITION_EXPIRATION_DAYS_KEY)).map(Long::getLong).orElse(null);

        final List<BigqueryLabel> labels = Optional.ofNullable(datastreamMetadata.get(METADATA_LABELS_KEY))
            .map(admin::parseLabelsString).orElse(null);
        final Integer schemaId = Optional.ofNullable(datastreamMetadata.get(METADATA_SCHEMA_ID_KEY)).map(Integer::valueOf).orElse(null);

        when(bigqueryDatastreamConfigurationFactory.createBigqueryDatastreamConfiguration(
                deadLetterTable,
                datastreamName,
                schemaRegistryUrl,
                BigquerySchemaEvolverFactory.createBigquerySchemaEvolver(BigquerySchemaEvolverType.dynamic),
                true,
                null,
                null,
                null,
                null
        )).thenReturn(expectedConfiguration.getDeadLetterTableConfiguration().orElse(null));
        when(bigqueryDatastreamConfigurationFactory.createBigqueryDatastreamConfiguration(
                destination,
                datastreamName,
                schemaRegistryUrl,
                schemaEvolver,
                autoCreateTable,
                partitionExpirationDays,
                expectedConfiguration.getDeadLetterTableConfiguration().orElse(null),
                labels,
                schemaId
        )).thenReturn(expectedConfiguration);
        final BigqueryDatastreamConfiguration config = admin.getConfigurationFromDatastream(datastream);
        assertEquals(config, expectedConfiguration);
    }

}