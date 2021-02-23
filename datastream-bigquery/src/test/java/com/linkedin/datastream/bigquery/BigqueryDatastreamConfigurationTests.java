/*
 * Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */

package com.linkedin.datastream.bigquery;

import java.util.List;

import org.apache.avro.Schema;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;

import com.linkedin.datastream.bigquery.schema.BigquerySchemaEvolver;
import com.linkedin.datastream.serde.Deserializer;


import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;

public class BigqueryDatastreamConfigurationTests {

    @Test
    public void testEquals() {
        final BigqueryDatastreamDestination destination = new BigqueryDatastreamDestination("project", "dataset", "destination");
        final BigquerySchemaEvolver schemaEvolver = mock(BigquerySchemaEvolver.class);
        final boolean createDestinationTable = true;
        final Deserializer deserializer = mock(Deserializer.class);
        final Serializer serializer = mock(Serializer.class);
        final long partitionExpirationDays = 5;
        final List<BigqueryLabel> labels = ImmutableList.of(BigqueryLabel.of("test"));
        final Schema fixedSchema = mock(Schema.class);
        final BigqueryDatastreamDestination deadLetterTableDestination = new BigqueryDatastreamDestination("project", "dataset", "dlq");
        final BigqueryDatastreamConfiguration deadLetterTableConfig = BigqueryDatastreamConfiguration
                .builder(deadLetterTableDestination, schemaEvolver, createDestinationTable, deserializer, serializer)
                .build();
        final BigqueryDatastreamConfiguration config = BigqueryDatastreamConfiguration.builder(destination, schemaEvolver, createDestinationTable, deserializer,
                serializer)
                .withPartitionExpirationDays(partitionExpirationDays)
                .withLabels(labels)
                .withFixedSchema(fixedSchema)
                .withDeadLetterTableConfiguration(deadLetterTableConfig)
                .build();

        assertEquals(BigqueryDatastreamConfiguration.builder(destination, schemaEvolver, createDestinationTable, deserializer,
                serializer)
                .withPartitionExpirationDays(partitionExpirationDays)
                .withLabels(labels)
                .withFixedSchema(fixedSchema)
                .withDeadLetterTableConfiguration(BigqueryDatastreamConfiguration
                        .builder(deadLetterTableDestination, schemaEvolver, createDestinationTable, deserializer, serializer)
                        .build())
                .build(), config);
    }

    @Test
    public void testHashCode() {
        final BigqueryDatastreamDestination destination = new BigqueryDatastreamDestination("project", "dataset", "destination");
        final BigquerySchemaEvolver schemaEvolver = mock(BigquerySchemaEvolver.class);
        final boolean createDestinationTable = true;
        final Deserializer deserializer = mock(Deserializer.class);
        final Serializer serializer = mock(Serializer.class);
        final long partitionExpirationDays = 5;
        final List<BigqueryLabel> labels = ImmutableList.of(BigqueryLabel.of("test"));
        final Schema fixedSchema = mock(Schema.class);
        final BigqueryDatastreamDestination deadLetterTableDestination = new BigqueryDatastreamDestination("project", "dataset", "dlq");
        final BigqueryDatastreamConfiguration deadLetterTableConfig = BigqueryDatastreamConfiguration
                .builder(deadLetterTableDestination, schemaEvolver, createDestinationTable, deserializer, serializer)
                .build();
        final BigqueryDatastreamConfiguration config = BigqueryDatastreamConfiguration.builder(destination, schemaEvolver, createDestinationTable, deserializer,
                serializer)
                .withPartitionExpirationDays(partitionExpirationDays)
                .withLabels(labels)
                .withFixedSchema(fixedSchema)
                .withDeadLetterTableConfiguration(deadLetterTableConfig)
                .build();

        assertEquals(BigqueryDatastreamConfiguration.builder(destination, schemaEvolver, createDestinationTable, deserializer,
                serializer)
                .withPartitionExpirationDays(partitionExpirationDays)
                .withLabels(labels)
                .withFixedSchema(fixedSchema)
                .withDeadLetterTableConfiguration(BigqueryDatastreamConfiguration
                        .builder(deadLetterTableDestination, schemaEvolver, createDestinationTable, deserializer, serializer)
                        .build())
                .build().hashCode(), config.hashCode());
    }
}
