/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.bigquery;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.datastream.bigquery.schema.BigquerySchemaEvolver;
import com.linkedin.datastream.bigquery.schema.BigquerySchemaEvolverFactory;
import com.linkedin.datastream.bigquery.schema.BigquerySchemaEvolverType;
import com.linkedin.datastream.bigquery.schema.SimpleBigquerySchemaEvolver;
import com.linkedin.datastream.bigquery.translator.SchemaTranslator;
import com.linkedin.datastream.common.Package;
import com.linkedin.datastream.common.Record;
import com.linkedin.datastream.metrics.DynamicMetricsManager;
import com.linkedin.datastream.serde.Deserializer;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.time.Instant;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class BatchTests {

    @BeforeClass
    public static void beforeClass() {
        DynamicMetricsManager.createInstance(new MetricRegistry(), BigqueryBatchCommitterTests.class.getSimpleName());
    }

    private MockSchemaRegistryClient schemaRegistryClient;
    private KafkaAvroSerializer serializer;
    private BigqueryBatchCommitter committer;
    private BigquerySchemaEvolver schemaEvolver;
    private Batch batch;

    @BeforeMethod
    void beforeTest() {
        schemaRegistryClient = new MockSchemaRegistryClient();
        final Deserializer deserializer = new KafkaDeserializer(new KafkaAvroDeserializer(schemaRegistryClient));
        serializer = new KafkaAvroSerializer(schemaRegistryClient);
        committer = mock(BigqueryBatchCommitter.class);
        schemaEvolver = BigquerySchemaEvolverFactory.createBigquerySchemaEvolver(BigquerySchemaEvolverType.simple);
        batch = new Batch(10, 10000, 1, deserializer, committer, schemaEvolver);
    }

    @Test
    public void testInitSchemaOnFirstWrite() throws InterruptedException, IOException, RestClientException {
        final org.apache.avro.Schema avroSchema = SchemaBuilder.builder("com.linkedin")
                .record("test_message").fields().name("message").type("string").noDefault()
                .endRecord();

        final GenericRecord record = new GenericRecordBuilder(avroSchema)
                .set("message", "test")
                .build();

        final String topicName = "testTopic";

        schemaRegistryClient.register(topicName + "-value", avroSchema);

        final String projectId = "project_name";
        final String datasetName = "dataset_name";
        final String tableName = "table_name";
        final BigqueryDatastreamDestination destination = new BigqueryDatastreamDestination(projectId, datasetName, tableName);

        final Package aPackage = new Package.PackageBuilder()
                .setTopic(topicName)
                .setDestination(destination.toString())
                .setOffset("0")
                .setPartition("0")
                .setCheckpoint("test")
                .setTimestamp(Instant.now().toEpochMilli())
                .setRecord(new Record(null, serializer.serialize(topicName, record)))
                .build();

        batch.write(aPackage);

        final com.google.cloud.bigquery.Schema bqSchema = SchemaTranslator.translate(avroSchema);
        verify(committer).setDestTableSchema(destination, bqSchema);
    }

    @Test
    public void testEvolveSchemaOnWrite() throws InterruptedException, IOException, RestClientException {
        final org.apache.avro.Schema avroSchema = SchemaBuilder.builder("com.linkedin")
                .record("test_message").fields().name("message").type("string").noDefault()
                .endRecord();
        {
            final GenericRecord record = new GenericRecordBuilder(avroSchema)
                    .set("message", "test")
                    .build();

            final String topicName = "testTopic";

            schemaRegistryClient.register(topicName + "-value", avroSchema);

            final String projectId = "project_name";
            final String datasetName = "dataset_name";
            final String tableName = "table_name";
            final BigqueryDatastreamDestination destination = new BigqueryDatastreamDestination(projectId, datasetName, tableName);

            final Package aPackage = new Package.PackageBuilder()
                    .setTopic(topicName)
                    .setDestination(destination.toString())
                    .setOffset("0")
                    .setPartition("0")
                    .setCheckpoint("test")
                    .setTimestamp(Instant.now().toEpochMilli())
                    .setRecord(new Record(null, serializer.serialize(topicName, record)))
                    .build();

            batch.write(aPackage);

            final com.google.cloud.bigquery.Schema bqSchema = SchemaTranslator.translate(avroSchema);
            verify(committer).setDestTableSchema(destination, bqSchema);
        }
        final org.apache.avro.Schema newAvroSchema = SchemaBuilder.builder("com.linkedin")
                .record("test_message").fields()
                    .name("message").type("string").noDefault()
                    .name("new_int").type("int").noDefault()
                .endRecord();

        final GenericRecord record = new GenericRecordBuilder(newAvroSchema)
                .set("message", "test")
                .set("new_int", 123)
                .build();

        final String topicName = "testTopic";

        schemaRegistryClient.register(topicName + "-value", newAvroSchema);

        final String projectId = "project_name";
        final String datasetName = "dataset_name";
        final String tableName = "table_name";
        final BigqueryDatastreamDestination destination = new BigqueryDatastreamDestination(projectId, datasetName, tableName);

        final Package aPackage = new Package.PackageBuilder()
                .setTopic(topicName)
                .setDestination(destination.toString())
                .setOffset("1")
                .setPartition("0")
                .setCheckpoint("test")
                .setTimestamp(Instant.now().toEpochMilli())
                .setRecord(new Record(null, serializer.serialize(topicName, record)))
                .build();

        batch.write(aPackage);

        final com.google.cloud.bigquery.Schema bqSchema = schemaEvolver.evolveSchema(SchemaTranslator.translate(avroSchema), SchemaTranslator.translate(newAvroSchema));
        verify(committer).setDestTableSchema(destination, bqSchema);

    }
}
