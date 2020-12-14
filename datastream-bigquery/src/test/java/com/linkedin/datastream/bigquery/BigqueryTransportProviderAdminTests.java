/*
 * Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */

package com.linkedin.datastream.bigquery;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

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

import static com.linkedin.datastream.bigquery.BigqueryTransportProviderAdmin.METADATA_LABELS_KEY;
import static org.mockito.Mockito.mock;

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
    private Map<String, String> defaultMetadata;
    private BigquerySchemaEvolver defaultSchemaEvolver;
    private Map<BigqueryDatastreamDestination, BigqueryDatastreamConfiguration> datastreamConfigByDestination;
    private Map<String, BigquerySchemaEvolver> bigquerySchemaEvolverMap;
    private BigqueryTransportProviderFactory bigqueryTransportProviderFactory;

    @BeforeMethod
    public void beforeTest() {
        bufferedTransportProvider = mock(BigqueryBufferedTransportProvider.class);
        serializer = mock(Serializer.class);
        deserializer = mock(Deserializer.class);
        defaultMetadata = new HashMap<>();
        defaultSchemaEvolver = mock(BigquerySchemaEvolver.class);
        datastreamConfigByDestination = new HashMap<>();
        bigquerySchemaEvolverMap = new HashMap<>();
        bigqueryTransportProviderFactory = mock(BigqueryTransportProviderFactory.class);
    }

    @Test
    public void testAssignTask() {
        final BigqueryTransportProviderAdmin admin = new BigqueryTransportProviderAdmin(
                bufferedTransportProvider,
                serializer,
                deserializer,
                defaultMetadata,
                defaultSchemaEvolver,
                datastreamConfigByDestination,
                bigquerySchemaEvolverMap,
                bigqueryTransportProviderFactory
        );
        final Datastream datastream = DatastreamTestUtils.createDatastreamWithoutDestination("connector", "test", "source");
        final DatastreamTask task = new DatastreamTaskImpl(Collections.singletonList(datastream));
        final BigqueryDatastreamConfiguration config = admin.getConfigurationFromDatastreamTask(task);
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
                serializer,
                deserializer,
                defaultMetadata,
                defaultSchemaEvolver,
                datastreamConfigByDestination,
                bigquerySchemaEvolverMap,
                bigqueryTransportProviderFactory
        );
        final Datastream datastream = DatastreamTestUtils.createDatastreamWithoutDestination("connector", "test", "source");
        final DatastreamTask task = new DatastreamTaskImpl(Collections.singletonList(datastream));
        final BigqueryDatastreamConfiguration config = admin.getConfigurationFromDatastreamTask(task);
        final BigqueryTransportProvider bigqueryTransportProvider = mock(BigqueryTransportProvider.class);
        when(bigqueryTransportProvider.getDatastreamConfiguration()).thenReturn(config);
        final Set<BigqueryDatastreamDestination> destinations = new HashSet<>();
        final BigqueryDatastreamDestination destination = new BigqueryDatastreamDestination("project", "dataset", "dest");
        destinations.add(destination);
        datastreamConfigByDestination.put(destination, config);

        when(bigqueryTransportProvider.getDestinations()).thenReturn(destinations);
        when(bigqueryTransportProviderFactory.createTransportProvider(bufferedTransportProvider, serializer, deserializer, config, datastreamConfigByDestination
        )).thenReturn(bigqueryTransportProvider);

        final TransportProvider transportProvider = admin.assignTransportProvider(task);

        assertEquals(transportProvider, bigqueryTransportProvider);
        assertEquals(admin.getDatastreamTransportProviders().get(datastream), transportProvider);
        assertEquals(admin.getTransportProviderTasks().get(transportProvider), Collections.singleton(task));

        admin.unassignTransportProvider(task);

        assertFalse(admin.getDatastreamTransportProviders().containsKey(datastream));
        assertFalse(admin.getTransportProviderTasks().containsKey(transportProvider));
        verify(bigqueryTransportProvider).close();
        verify(bigqueryTransportProvider).getDestinations();
        assertTrue(datastreamConfigByDestination.isEmpty());
    }

    @DataProvider(name = "datastream config test cases")
    public Object[][] datastreamConfigTestCases() {
        return new Object[][] {
                {
                        BigquerySchemaEvolverFactory.createBigquerySchemaEvolver(BigquerySchemaEvolverType.noop),
                        Collections.emptyMap(), Collections.emptyMap(),
                        new BigqueryDatastreamConfiguration(BigquerySchemaEvolverFactory.createBigquerySchemaEvolver(BigquerySchemaEvolverType.noop), true)
                },
                {
                        BigquerySchemaEvolverFactory.createBigquerySchemaEvolver(BigquerySchemaEvolverType.simple),
                        Collections.emptyMap(), Collections.emptyMap(),
                        new BigqueryDatastreamConfiguration(BigquerySchemaEvolverFactory.createBigquerySchemaEvolver(BigquerySchemaEvolverType.simple), true)
                },
                {
                        BigquerySchemaEvolverFactory.createBigquerySchemaEvolver(BigquerySchemaEvolverType.simple),
                        ImmutableMap.of(
                                METADATA_LABELS_KEY, "test,name:value"
                        ), Collections.emptyMap(),
                        new BigqueryDatastreamConfiguration(BigquerySchemaEvolverFactory.createBigquerySchemaEvolver(BigquerySchemaEvolverType.simple), true,
                                null, null, null, ImmutableList.of(BigqueryLabel.of("test"), BigqueryLabel.of("name", "value")))
                },
                {
                        BigquerySchemaEvolverFactory.createBigquerySchemaEvolver(BigquerySchemaEvolverType.simple),
                        ImmutableMap.of(
                        ), ImmutableMap.of(
                            METADATA_LABELS_KEY, "test,name:value"
                        ),
                        new BigqueryDatastreamConfiguration(BigquerySchemaEvolverFactory.createBigquerySchemaEvolver(BigquerySchemaEvolverType.simple), true,
                                null, null, null, ImmutableList.of(BigqueryLabel.of("test"), BigqueryLabel.of("name", "value")))
                }
        };
    }

    @Test(dataProvider = "datastream config test cases")
    public void testGetConfigurationFromDatastream(final BigquerySchemaEvolver defaultSchemaEvolver,
                                                   final Map<String, String> defaultMetadata,
                                                   final Map<String, String> datastreamMetadata,
                                                   final BigqueryDatastreamConfiguration expectedConfiguration) {
        final BigqueryTransportProviderAdmin admin = new BigqueryTransportProviderAdmin(
                bufferedTransportProvider,
                serializer,
                deserializer,
                defaultMetadata,
                defaultSchemaEvolver,
                datastreamConfigByDestination,
                bigquerySchemaEvolverMap,
                bigqueryTransportProviderFactory
        );
        final Datastream datastream = DatastreamTestUtils.createDatastreamWithoutDestination("connector", "test", "source");
        datastream.setMetadata(new StringMap(datastreamMetadata));
        final BigqueryDatastreamConfiguration config = admin.getConfigurationFromDatastream(datastream);
        assertEquals(config, expectedConfiguration);
    }
}