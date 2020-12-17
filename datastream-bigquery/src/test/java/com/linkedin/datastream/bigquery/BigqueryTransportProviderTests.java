/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.bigquery;

import com.codahale.metrics.MetricRegistry;
import com.google.api.client.util.DateTime;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import com.linkedin.datastream.bigquery.schema.BigquerySchemaEvolverFactory;
import com.linkedin.datastream.bigquery.schema.BigquerySchemaEvolverType;
import com.linkedin.datastream.bigquery.translator.RecordTranslator;
import com.linkedin.datastream.bigquery.translator.SchemaTranslator;
import com.linkedin.datastream.common.BrooklinEnvelope;
import com.linkedin.datastream.common.BrooklinEnvelopeMetadataConstants;
import com.linkedin.datastream.common.DatastreamRecordMetadata;
import com.linkedin.datastream.common.SendCallback;
import com.linkedin.datastream.metrics.DynamicMetricsManager;
import com.linkedin.datastream.serde.Deserializer;
import com.linkedin.datastream.server.DatastreamProducerRecord;
import com.linkedin.datastream.server.DatastreamProducerRecordBuilder;
import com.linkedin.datastream.server.Pair;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.mockito.Matchers.any;
import static com.linkedin.datastream.server.api.transport.buffered.AbstractBufferedTransportProvider.KAFKA_ORIGIN_CLUSTER;
import static com.linkedin.datastream.server.api.transport.buffered.AbstractBufferedTransportProvider.KAFKA_ORIGIN_OFFSET;
import static com.linkedin.datastream.server.api.transport.buffered.AbstractBufferedTransportProvider.KAFKA_ORIGIN_PARTITION;
import static com.linkedin.datastream.server.api.transport.buffered.AbstractBufferedTransportProvider.KAFKA_ORIGIN_TOPIC;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class BigqueryTransportProviderTests {

    private static final AtomicInteger TOPIC_COUNTER = new AtomicInteger();
    private static final AtomicInteger DATASET_COUNTER = new AtomicInteger();

    @BeforeClass
    public static void setup() {
        DynamicMetricsManager.createInstance(new MetricRegistry(), BigqueryTransportProviderTests.class.getSimpleName());
    }

    @Test
    public void testSendHappyPath() {
        final int maxBatchAge = 10;
        final int maxBatchSize = 10;
        final int maxInflightCommits = 10;
        final int queueSize = 10;
        final int totalEvents = 25;

        final Schema schema = SchemaBuilder.builder("com.linkedin").record("test_message")
                .fields().name("message").type("string").noDefault().endRecord();
        final List<Map<String, ?>> data = IntStream.range(0, totalEvents)
                .mapToObj(i -> ImmutableMap.of("message", "payload " + i))
                .collect(Collectors.toList());

        final List<GenericRecord> events = data.stream()
                .map(recordData -> {
                    final GenericRecordBuilder builder = new GenericRecordBuilder(schema);
                    recordData.forEach(builder::set);
                    return builder.build();
                }).collect(Collectors.toList());


        final String projectId = "projectId";
        final String datasetName = getUniqueDatasetName();
        final String cluster = "kafka://test";
        final String topicName = getUniqueTopicName();
        final String tableName = BigqueryBatchCommitter.sanitizeTableName(topicName);
        final BigqueryDatastreamDestination destination = new BigqueryDatastreamDestination(projectId, datasetName, tableName);
        final Map<BigqueryDatastreamDestination, BigqueryDatastreamConfiguration> destConfigs = new HashMap<>();
        final BigqueryDatastreamConfiguration config = new BigqueryDatastreamConfiguration(BigquerySchemaEvolverFactory.createBigquerySchemaEvolver(BigquerySchemaEvolverType.simple), true);
        destConfigs.put(destination, config);

        final BigqueryBatchCommitter committer = new BigqueryBatchCommitter(bigQuery, 1, destConfigs);
        final BatchBuilder batchBuilder = new BatchBuilder(
                maxBatchSize, maxBatchAge, maxInflightCommits, committer, queueSize, valueDeserializer, destConfigs
        );
        final List<BatchBuilder> batchBuilders = ImmutableList.of(batchBuilder);

        final TableId tableId = TableId.of(projectId, datasetName, tableName);
        final TableId insertTableId = TableId.of(projectId, tableId.getDataset(),
                String.format("%s$%s", tableId.getTable(), LocalDate.now(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern("yyyyMMdd"))));

        final InsertAllResponse insertAllResponse = mock(InsertAllResponse.class);
        when(insertAllResponse.getInsertErrors()).thenReturn(ImmutableMap.of());
        when(bigQuery.insertAll(any(InsertAllRequest.class))).thenReturn(insertAllResponse);

        final DateTime eventTimestamp = new DateTime(new Date(System.currentTimeMillis()));

        try (final BigqueryTransportProvider transportProvider = new BigqueryTransportProvider(
                new BigqueryBufferedTransportProvider("test", batchBuilders, committer, maxBatchAge),
                valueSerializer, valueDeserializer, config, destConfigs)) {
            sendEvents(transportProvider, destination.toString(), cluster, topicName, 0, events, eventTimestamp, null);
        }

        final ArgumentCaptor<InsertAllRequest> requestArgumentCaptor = ArgumentCaptor.forClass(InsertAllRequest.class);
        verify(bigQuery, atLeastOnce()).insertAll(requestArgumentCaptor.capture());
        final List<InsertAllRequest> capturedRequests = requestArgumentCaptor.getAllValues();

        capturedRequests.forEach(request -> assertEquals(request.getTable(), insertTableId));
        final List<Map<String, Object>> insertedData = capturedRequests.stream()
                .flatMap(request -> request.getRows().stream().map(row -> row.getContent().entrySet().stream()
                        .filter(entry -> !"__metadata".equals(entry.getKey()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
                )).collect(Collectors.toList());

        assertEquals(insertedData, data);
    }

    private static InsertAllRequest.RowToInsert getExpectedRowToInsert(final InsertAllRequest.RowToInsert actualRow, final GenericRecord expectedRecord) {
        return RecordTranslator.translate(expectedRecord, (TableRow) actualRow.getContent().get("__metadata"));
    }

    private static InsertAllRequest getExpectedRequest(final InsertAllRequest actualRequest, final TableId insertTableId,
                                                       final List<GenericRecord> expectedRecords) {
        assertEquals(actualRequest.getRows().size(), expectedRecords.size());
        return InsertAllRequest.of(insertTableId, IntStream.range(0, expectedRecords.size())
                .mapToObj(index -> getExpectedRowToInsert(actualRequest.getRows().get(index), expectedRecords.get(index)))
                .collect(Collectors.toList()));
    }

    @Test
    public void testSendAndEvolveSchema() {
        final int maxBatchAge = 10;
        final int maxBatchSize = 10;
        final int maxInflightCommits = 10;
        final int queueSize = 10;

        final Schema schema1 = SchemaBuilder.builder("com.linkedin").record("test_message")
                .fields().name("message").type("string").noDefault().endRecord();

        final List<Map<String, ?>> data1 = IntStream.range(0, 15)
                .mapToObj(i -> ImmutableMap.of("message", "payload " + i))
                .collect(Collectors.toList());
        final List<GenericRecord> events1 = data1.stream()
                .map(recordData -> {
                    final GenericRecordBuilder builder = new GenericRecordBuilder(schema1);
                    recordData.forEach(builder::set);
                    return builder.build();
                }).collect(Collectors.toList());


        final Schema schema2 = SchemaBuilder.builder("com.linkedin").record("test_message")
                .fields()
                .name("message").type("string").noDefault()
                .name("new_message").type("string").noDefault()
                .endRecord();
        final List<Map<String, ?>> data2 = IntStream.range(0, 15)
                .mapToObj(i -> ImmutableMap.of("message", "payload " + i, "new_message", "new payload " + i))
                .collect(Collectors.toList());
        final List<GenericRecord> events2 = data2.stream()
                .map(recordData -> {
                    final GenericRecordBuilder builder = new GenericRecordBuilder(schema2);
                    recordData.forEach(builder::set);
                    return builder.build();
                }).collect(Collectors.toList());

        final String projectId = "projectId";
        final String datasetName = getUniqueDatasetName();
        final String cluster = "kafka://test";
        final String topicName = getUniqueTopicName();
        final String tableName = BigqueryBatchCommitter.sanitizeTableName(topicName);
        final BigqueryDatastreamDestination destination = new BigqueryDatastreamDestination(projectId, datasetName, tableName);
        final BigqueryDatastreamConfiguration config = new BigqueryDatastreamConfiguration(BigquerySchemaEvolverFactory.createBigquerySchemaEvolver(BigquerySchemaEvolverType.simple), true);
        final Map<BigqueryDatastreamDestination, BigqueryDatastreamConfiguration> destConfigs = new HashMap<>();
        destConfigs.put(destination, new BigqueryDatastreamConfiguration(BigquerySchemaEvolverFactory.createBigquerySchemaEvolver(BigquerySchemaEvolverType.simple), true));

        final BigqueryBatchCommitter committer = new BigqueryBatchCommitter(bigQuery, 1, destConfigs);
        final BatchBuilder batchBuilder = new BatchBuilder(
                maxBatchSize, maxBatchAge, maxInflightCommits, committer, queueSize, valueDeserializer, destConfigs
        );
        final List<BatchBuilder> batchBuilders = ImmutableList.of(batchBuilder);
        final TableId tableId = TableId.of(projectId, datasetName, topicName);

        final com.google.cloud.bigquery.Schema firstSchema = SchemaTranslator.translate(schema1);
        final TableDefinition tableDefinition = StandardTableDefinition.newBuilder()
                .setSchema(firstSchema)
                .setTimePartitioning(TimePartitioning.of(TimePartitioning.Type.DAY))
                .build();

        final Table mockedTable = mock(Table.class);
        when(mockedTable.getDefinition()).thenReturn(tableDefinition);

        final Table.Builder tableBuilder = mock(Table.Builder.class);
        when(mockedTable.toBuilder()).thenReturn(tableBuilder);
        final TableDefinition evolvedTableDefinition = StandardTableDefinition.newBuilder()
                .setSchema(config.getSchemaEvolver().evolveSchema(firstSchema, SchemaTranslator.translate(schema2)))
                .setTimePartitioning(TimePartitioning.of(TimePartitioning.Type.DAY))
                .build();
        when(tableBuilder.setDefinition(evolvedTableDefinition)).thenReturn(tableBuilder);
        final Table evolvedTable = mock(Table.class);
        when(evolvedTable.getDefinition()).thenReturn(evolvedTableDefinition);
        when(tableBuilder.build()).thenReturn(evolvedTable);
        final AtomicBoolean tableUpdated = new AtomicBoolean(false);
        when(bigQuery.getTable(tableId)).thenReturn(mockedTable);
        final List<Map<String, Object>> insertedData = new LinkedList<>();
        when(bigQuery.insertAll(any(InsertAllRequest.class))).then(invocation -> {
            final InsertAllRequest request = invocation.getArgumentAt(0, InsertAllRequest.class);
            if (request.getRows().stream().anyMatch(row -> row.getContent().containsKey("new_message"))) {
                if (tableUpdated.get()) {
                    request.getRows().forEach(row -> insertedData.add(row.getContent().entrySet().stream()
                            .filter(entry -> !"__metadata".equals(entry.getKey()))
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))));
                    final InsertAllResponse response = mock(InsertAllResponse.class);
                    when(response.getInsertErrors()).thenReturn(ImmutableMap.of());
                    return response;
                } else {
                    throw new BigQueryException(400, "Table missing column", new BigQueryError("invalid", "new_message", "Table missing column"));
                }
            } else {
                request.getRows().forEach(row -> insertedData.add(row.getContent().entrySet().stream()
                        .filter(entry -> !"__metadata".equals(entry.getKey()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))));
                final InsertAllResponse response = mock(InsertAllResponse.class);
                when(response.getInsertErrors()).thenReturn(ImmutableMap.of());
                return response;
            }
        });
        when(evolvedTable.update()).then(invocation -> {
            if (!tableUpdated.get()) {
                tableUpdated.set(true);
                return mock(Table.class);
            } else {
                throw new IllegalStateException("Table already updated");
            }
        });

        try (final BigqueryTransportProvider transportProvider = new BigqueryTransportProvider(
                new BigqueryBufferedTransportProvider("test", batchBuilders, committer, maxBatchAge),
                valueSerializer, valueDeserializer, config, destConfigs)) {
            final DateTime eventTimestamp1 = new DateTime(new Date(System.currentTimeMillis()));
            sendEvents(transportProvider, destination.toString(), cluster, topicName, 0, events1, eventTimestamp1, null);

            verify(bigQuery, atLeastOnce()).insertAll(any(InsertAllRequest.class));

            final DateTime eventTimestamp2 = new DateTime(new Date(System.currentTimeMillis()));
            sendEvents(transportProvider, destination.toString(), cluster, topicName, 0, events2, eventTimestamp2, null);

            verify(evolvedTable).update();
            verify(bigQuery, atLeastOnce()).insertAll(any(InsertAllRequest.class));

            assertEquals(insertedData, Stream.concat(data1.stream(), data2.stream()).collect(Collectors.toList()));
        }
    }

    @Test
    public void testInsertRecordExceptionHandling() {
        final int maxBatchAge = 10;
        final int maxBatchSize = 10;
        final int maxInflightCommits = 10;
        final int queueSize = 10;
        final int totalEvents = 15;

        final Schema schema = SchemaBuilder.builder("com.linkedin").record("test_message")
                .fields().name("message").type("string").noDefault().endRecord();
        final List<GenericRecord> events = IntStream.range(0, totalEvents)
                .mapToObj(i -> new GenericRecordBuilder(schema).set("message", "payload " + i).build())
                .collect(Collectors.toList());

        final String projectId = "projectId";
        final String datasetName = getUniqueDatasetName();
        final String cluster = "kafka://test";
        final String topicName = getUniqueTopicName();
        final String tableName = BigqueryBatchCommitter.sanitizeTableName(topicName);
        final BigqueryDatastreamDestination destination = new BigqueryDatastreamDestination(projectId, datasetName, tableName);
        final Map<BigqueryDatastreamDestination, BigqueryDatastreamConfiguration> destConfigs = new HashMap<>();
        final BigqueryDatastreamConfiguration config = new BigqueryDatastreamConfiguration(BigquerySchemaEvolverFactory.createBigquerySchemaEvolver(BigquerySchemaEvolverType.simple), true);
        destConfigs.put(destination, config);

        final BigqueryBatchCommitter committer = new BigqueryBatchCommitter(bigQuery, 1, destConfigs);
        final BatchBuilder batchBuilder = new BatchBuilder(
                maxBatchSize, maxBatchAge, maxInflightCommits, committer, queueSize, valueDeserializer, destConfigs
        );
        final List<BatchBuilder> batchBuilders = ImmutableList.of(batchBuilder);

        final TableId tableId = TableId.of(projectId, datasetName, topicName);
        final TableId insertTableId = TableId.of(tableId.getProject(), tableId.getDataset(),
                String.format("%s$%s", tableId.getTable(), LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"))));

        final TableDefinition tableDefinition = StandardTableDefinition.newBuilder()
                .setSchema(SchemaTranslator.translate(schema))
                .setTimePartitioning(TimePartitioning.of(TimePartitioning.Type.DAY))
                .build();
        final TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
        final Table mockedTable = mock(Table.class);
        when(mockedTable.getDefinition()).thenReturn(tableDefinition);
        when(bigQuery.getTable(tableId)).thenReturn(mockedTable);

        final TableId exceptionsTableId = TableId.of(projectId, datasetName, topicName + "_exceptions");

        final TableDefinition exceptionsTableDefinition = StandardTableDefinition.newBuilder()
                .setSchema(SchemaTranslator.translate(BigqueryTransportProvider.EXCEPTION_RECORD_SCHEMA))
                .setTimePartitioning(TimePartitioning.of(TimePartitioning.Type.DAY))
                .build();
        final TableInfo exceptionsTableInfo = TableInfo.newBuilder(exceptionsTableId, exceptionsTableDefinition).build();
        final Table mockedExceptionsTable = mock(Table.class);
        when(mockedExceptionsTable.getDefinition()).thenReturn(exceptionsTableDefinition);
        when(bigQuery.getTable(exceptionsTableId)).thenReturn(mockedExceptionsTable);

        final Exception testException = new RuntimeException("Test insert failure");
        when(bigQuery.insertAll(any(InsertAllRequest.class)))
                .thenThrow(testException);

        final DateTime eventTimestamp = new DateTime(new Date(System.currentTimeMillis()));
        final List<Exception> callbackExceptions = new LinkedList<>();

        try (final BigqueryTransportProvider transportProvider = new BigqueryTransportProvider(
                new BigqueryBufferedTransportProvider("test", batchBuilders, committer, maxBatchAge),
                valueSerializer, valueDeserializer, config, destConfigs)) {
            sendEvents(transportProvider, destination.toString(), cluster, topicName, 0, events, eventTimestamp, (metadata, exception) -> {
                if (exception != null) {
                    callbackExceptions.add(exception);
                }
            });
        }

        verify(bigQuery, never()).create(tableInfo);
        verify(bigQuery, never()).create(exceptionsTableInfo);

        final ArgumentCaptor<InsertAllRequest> requestArgumentCaptor = ArgumentCaptor.forClass(InsertAllRequest.class);
        verify(bigQuery, atLeastOnce()).insertAll(requestArgumentCaptor.capture());
        final List<InsertAllRequest> capturedRequests = requestArgumentCaptor.getAllValues();

        int rowsChecked = 0;
        for (final InsertAllRequest request : capturedRequests) {
            final int actualRequestRowCount = request.getRows().size();
            assertEquals(request, getExpectedRequest(request, insertTableId, events.subList(rowsChecked, rowsChecked + actualRequestRowCount)));
            rowsChecked += actualRequestRowCount;
        }
        assertEquals(rowsChecked, events.size());
        assertEquals(callbackExceptions.size(), totalEvents);
    }


    @Test
    public void testSchemaEvolutionExceptionHandling() {
        final int maxBatchAge = 10;
        final int maxBatchSize = 10;
        final int maxInflightCommits = 10;
        final int queueSize = 10;

        final Schema currentSchema = SchemaBuilder.builder("com.linkedin").record("test_message")
                .fields().requiredInt("message").endRecord();

        final Schema incompatibleEvolvedSchema = SchemaBuilder.builder("com.linkedin").record("test_message")
                .fields()
                .requiredString("message")
                .endRecord();
        final List<GenericRecord> incompatibleEvents = IntStream.range(0, 15)
                .mapToObj(i -> new GenericRecordBuilder(incompatibleEvolvedSchema)
                        .set("message", "payload " + i).build())
                .collect(Collectors.toList());

        final String projectId = "projectId";
        final String datasetName = getUniqueDatasetName();
        final String cluster = "kafka://test";
        final String topicName = getUniqueTopicName();
        final String tableName = BigqueryBatchCommitter.sanitizeTableName(topicName);
        final BigqueryDatastreamDestination destination = new BigqueryDatastreamDestination(projectId, datasetName, tableName);
        final Map<BigqueryDatastreamDestination, BigqueryDatastreamConfiguration> destConfigs = new HashMap<>();
        final BigqueryDatastreamConfiguration config = new BigqueryDatastreamConfiguration(BigquerySchemaEvolverFactory.createBigquerySchemaEvolver(BigquerySchemaEvolverType.simple), true,
                null, null, new BigqueryDatastreamConfiguration(BigquerySchemaEvolverFactory.createBigquerySchemaEvolver(BigquerySchemaEvolverType.simple), true), null);
        destConfigs.put(destination, config);

        final BigqueryBatchCommitter committer = new BigqueryBatchCommitter(bigQuery, 1, destConfigs);
        final BatchBuilder batchBuilder = new BatchBuilder(
                maxBatchSize, maxBatchAge, maxInflightCommits, committer, queueSize, valueDeserializer, destConfigs
        );
        final List<BatchBuilder> batchBuilders = ImmutableList.of(batchBuilder);

        final TableId tableId = TableId.of(projectId, datasetName, topicName);
        final TableId insertTableId = TableId.of(tableId.getProject(), tableId.getDataset(),
                String.format("%s$%s", tableId.getTable(), LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"))));

        final com.google.cloud.bigquery.Schema firstSchema = SchemaTranslator.translate(currentSchema);
        final TableDefinition tableDefinition = StandardTableDefinition.newBuilder()
                .setSchema(firstSchema)
                .setTimePartitioning(TimePartitioning.of(TimePartitioning.Type.DAY))
                .build();
        final TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();

        final Table mockedTable = mock(Table.class);
        when(mockedTable.getDefinition()).thenReturn(tableDefinition);
        when(bigQuery.getTable(tableId)).thenReturn(mockedTable);

        final TableId exceptionsTableId = TableId.of(projectId, datasetName, topicName + "_exceptions");
        final TableId insertExceptionsTableId = TableId.of(tableId.getProject(), tableId.getDataset(),
                String.format("%s$%s", exceptionsTableId.getTable(), LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"))));

        final TableDefinition exceptionsTableDefinition = StandardTableDefinition.newBuilder()
                .setSchema(SchemaTranslator.translate(BigqueryTransportProvider.EXCEPTION_RECORD_SCHEMA))
                .setTimePartitioning(TimePartitioning.of(TimePartitioning.Type.DAY))
                .build();
        final TableInfo exceptionsTableInfo = TableInfo.newBuilder(exceptionsTableId, exceptionsTableDefinition).build();
        final Table mockedExceptionsTable = mock(Table.class);
        when(mockedExceptionsTable.getDefinition()).thenReturn(exceptionsTableDefinition);
        when(bigQuery.getTable(exceptionsTableId)).thenReturn(mockedExceptionsTable);

        final List<Map<String, Object>> insertedErrors = new LinkedList<>();
        when(bigQuery.insertAll(any(InsertAllRequest.class))).then(invocation -> {
            final InsertAllRequest request = invocation.getArgumentAt(0, InsertAllRequest.class);
            final InsertAllResponse response = mock(InsertAllResponse.class);
            final Map<Long, List<BigQueryError>> insertErrors;
            if (request.getTable().equals(insertTableId)) {
                insertErrors = IntStream.range(0, request.getRows().size()).boxed()
                        .flatMap(i -> {
                            final InsertAllRequest.RowToInsert row = request.getRows().get(i);
                            final Optional<BigQueryError> optionalError;
                            if (row.getContent().containsKey("message")) {
                                final Object message = row.getContent().get("message");
                                if (message instanceof Integer) {
                                    optionalError = Optional.empty();
                                } else {
                                    optionalError = Optional.of(new BigQueryError("invalid", "message", "Cannot convert value to an int"));
                                }
                            } else {
                                throw new IllegalStateException("Missing required column: message");
                            }
                            return optionalError.map(error -> Stream.of(Pair.of(i.longValue(), Collections.singletonList(error)))).orElse(Stream.empty());
                        }).collect(Collectors.toMap(Pair::getKey, Pair::getValue));

            } else if  (request.getTable().equals(insertExceptionsTableId)) {
                request.getRows().forEach(row -> insertedErrors.add(row.getContent()));
                insertErrors = Collections.emptyMap();
            } else {
                throw new IllegalStateException("Unexpected table id: " + request.getTable());
            }
            when(response.getInsertErrors()).thenReturn(insertErrors);
            return response;
        });

        final List<DatastreamRecordMetadata> callbackMetadata = new LinkedList<>();
        final List<Exception> callbackExceptions = new LinkedList<>();


        try (final BigqueryTransportProvider transportProvider = new BigqueryTransportProvider(
                new BigqueryBufferedTransportProvider("test", batchBuilders, committer, maxBatchAge),
                valueSerializer, valueDeserializer, config, destConfigs)) {

            final DateTime eventTimestamp = new DateTime(new Date(System.currentTimeMillis()));
            final String sourceCheckpoint = "testCheckpoint 1";
            final List<DatastreamProducerRecord> records = incompatibleEvents.stream()
                    .map(event -> createRecord(cluster, topicName, offsetIncrement.getAndIncrement(), 0, event, eventTimestamp, sourceCheckpoint))
                    .collect(Collectors.toList());
            sendEvents(transportProvider, destination.toString(), records, (metadata, exception) -> {
                callbackMetadata.add(metadata);
                if (exception != null) {
                    callbackExceptions.add(exception);
                }
            });
            final List<DatastreamRecordMetadata> expectedMetadata = records.stream()
                    .map(record -> new DatastreamRecordMetadata(sourceCheckpoint, topicName, 0))
                    .collect(Collectors.toList());
            assertEquals(
                    callbackMetadata,
                    expectedMetadata
            );
            verify(bigQuery, never()).create(tableInfo);
            verify(bigQuery, atLeastOnce()).getTable(tableId);
            verify(bigQuery, never()).create(exceptionsTableInfo);
            verify(bigQuery).getTable(exceptionsTableId);
            assertEquals(insertedErrors.size(), incompatibleEvents.size());
            assertEquals(callbackExceptions.size(), 0);
        }
    }


    @Test
    public void testInsertExceptionRecordFailure() {
        final int maxBatchAge = 10;
        final int maxBatchSize = 10;
        final int maxInflightCommits = 10;
        final int queueSize = 10;

        final Schema currentSchema = SchemaBuilder.builder("com.linkedin").record("test_message")
                .fields().requiredInt("message").endRecord();

        final Schema incompatibleEvolvedSchema = SchemaBuilder.builder("com.linkedin").record("test_message")
                .fields()
                .requiredString("message")
                .endRecord();
        final List<GenericRecord> incompatibleEvents = IntStream.range(0, 15)
                .mapToObj(i -> new GenericRecordBuilder(incompatibleEvolvedSchema)
                        .set("message", "payload " + i).build())
                .collect(Collectors.toList());

        final String projectId = "projectId";
        final String datasetName = getUniqueDatasetName();
        final String cluster = "kafka://test";
        final String topicName = getUniqueTopicName();
        final String tableName = BigqueryBatchCommitter.sanitizeTableName(topicName);
        final BigqueryDatastreamDestination destination = new BigqueryDatastreamDestination(projectId, datasetName, tableName);
        final Map<BigqueryDatastreamDestination, BigqueryDatastreamConfiguration> destConfigs = new HashMap<>();
        final BigqueryDatastreamConfiguration config = new BigqueryDatastreamConfiguration(BigquerySchemaEvolverFactory.createBigquerySchemaEvolver(BigquerySchemaEvolverType.simple), true,
                null, null, new BigqueryDatastreamConfiguration(BigquerySchemaEvolverFactory.createBigquerySchemaEvolver(BigquerySchemaEvolverType.simple), true), null);
        destConfigs.put(destination, config);

        final BigqueryBatchCommitter committer = new BigqueryBatchCommitter(bigQuery, 1, destConfigs);
        final BatchBuilder batchBuilder = new BatchBuilder(
                maxBatchSize, maxBatchAge, maxInflightCommits, committer, queueSize, valueDeserializer, destConfigs
        );
        final List<BatchBuilder> batchBuilders = ImmutableList.of(batchBuilder);

        final TableId tableId = TableId.of(projectId, datasetName, topicName);
        final TableId insertTableId = TableId.of(tableId.getProject(), tableId.getDataset(),
                String.format("%s$%s", tableId.getTable(), LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"))));

        final com.google.cloud.bigquery.Schema firstSchema = SchemaTranslator.translate(currentSchema);
        final TableDefinition tableDefinition = StandardTableDefinition.newBuilder()
                .setSchema(firstSchema)
                .setTimePartitioning(TimePartitioning.of(TimePartitioning.Type.DAY))
                .build();
        final TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();

        final Table mockedTable = mock(Table.class);
        when(mockedTable.getDefinition()).thenReturn(tableDefinition);
        when(bigQuery.getTable(tableId)).thenReturn(mockedTable);

        final TableId exceptionsTableId = TableId.of(projectId, datasetName, topicName + "_exceptions");
        final TableId insertExceptionsTableId = TableId.of(tableId.getProject(), tableId.getDataset(),
                String.format("%s$%s", exceptionsTableId.getTable(), LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"))));

        final TableDefinition exceptionsTableDefinition = StandardTableDefinition.newBuilder()
                .setSchema(SchemaTranslator.translate(BigqueryTransportProvider.EXCEPTION_RECORD_SCHEMA))
                .setTimePartitioning(TimePartitioning.of(TimePartitioning.Type.DAY))
                .build();
        final TableInfo exceptionsTableInfo = TableInfo.newBuilder(exceptionsTableId, exceptionsTableDefinition).build();
        final Table mockedExceptionsTable = mock(Table.class);
        when(mockedExceptionsTable.getDefinition()).thenReturn(exceptionsTableDefinition);
        when(bigQuery.getTable(exceptionsTableId)).thenReturn(mockedExceptionsTable);

        when(bigQuery.insertAll(any(InsertAllRequest.class))).then(invocation -> {
            final InsertAllRequest request = invocation.getArgumentAt(0, InsertAllRequest.class);
            final InsertAllResponse response = mock(InsertAllResponse.class);
            final Map<Long, List<BigQueryError>> insertErrors;
            if (request.getTable().equals(insertTableId)) {
                insertErrors = IntStream.range(0, request.getRows().size()).boxed()
                        .flatMap(i -> {
                            final InsertAllRequest.RowToInsert row = request.getRows().get(i);
                            final Optional<BigQueryError> optionalError;
                            if (row.getContent().containsKey("message")) {
                                final Object message = row.getContent().get("message");
                                if (message instanceof Integer) {
                                    optionalError = Optional.empty();
                                } else {
                                    optionalError = Optional.of(new BigQueryError("invalid", "message", "Cannot convert value to an int"));
                                }
                            } else {
                                throw new IllegalStateException("Missing required column: message");
                            }
                            return optionalError.map(error -> Stream.of(Pair.of(i.longValue(), Collections.singletonList(error)))).orElse(Stream.empty());
                        }).collect(Collectors.toMap(Pair::getKey, Pair::getValue));

            } else if  (request.getTable().equals(insertExceptionsTableId)) {
                insertErrors = IntStream.range(0, request.getRows().size()).boxed()
                        .collect(Collectors.toMap(Integer::longValue, i ->
                                Collections.singletonList(new BigQueryError("error", null, "Unexpected error"))));
            } else {
                throw new IllegalStateException("Unexpected table id: " + request.getTable());
            }
            when(response.getInsertErrors()).thenReturn(insertErrors);
            return response;
        });

        final List<DatastreamRecordMetadata> callbackMetadata = new LinkedList<>();
        final List<Exception> callbackExceptions = new LinkedList<>();


        try (final BigqueryTransportProvider transportProvider = new BigqueryTransportProvider(
                new BigqueryBufferedTransportProvider("test", batchBuilders, committer, maxBatchAge),
                valueSerializer, valueDeserializer, config, destConfigs)) {

            final DateTime eventTimestamp = new DateTime(new Date(System.currentTimeMillis()));
            final String sourceCheckpoint = "testCheckpoint 1";
            final List<DatastreamProducerRecord> records = incompatibleEvents.stream()
                    .map(event -> createRecord(cluster, topicName, offsetIncrement.getAndIncrement(), 0, event, eventTimestamp, sourceCheckpoint))
                    .collect(Collectors.toList());
            sendEvents(transportProvider, destination.toString(), records, (metadata, exception) -> {
                callbackMetadata.add(metadata);
                if (exception != null) {
                    callbackExceptions.add(exception);
                }
            });
            final List<DatastreamRecordMetadata> expectedMetadata = records.stream()
                    .map(record -> new DatastreamRecordMetadata(sourceCheckpoint, topicName, 0))
                    .collect(Collectors.toList());
            assertEquals(
                    callbackMetadata,
                    expectedMetadata
            );
            verify(bigQuery, never()).create(tableInfo);
            verify(bigQuery, atLeastOnce()).getTable(tableId);
            verify(bigQuery, never()).create(exceptionsTableInfo);
            verify(bigQuery, atLeastOnce()).getTable(exceptionsTableId);
            assertEquals(callbackExceptions.size(), incompatibleEvents.size());
        }
    }

    private KafkaAvroSerializer serializer;
    private Deserializer valueDeserializer;
    private Serializer valueSerializer;
    private BigQuery bigQuery;

    @BeforeMethod
    void beforeTest() {
        final MockSchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
        final String schemaRegistryUrl = "http://schema-registry/";
        serializer = new KafkaAvroSerializer(schemaRegistryClient, ImmutableMap.of(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl));
        serializer.configure(Collections.singletonMap("schema.registry.url", "http://schema-registry"), false);
        valueDeserializer = new KafkaDeserializer(new KafkaAvroDeserializer(schemaRegistryClient));
        valueSerializer = new KafkaSerializer(serializer);
        bigQuery = mock(BigQuery.class);
    }

    private void sendEvents(final BigqueryTransportProvider transportProvider, final String destination,
                            final String cluster, final String topicName, final int partition,
                            final List<GenericRecord> events, final DateTime eventTimestamp,
                            final SendCallback onComplete) {
        sendEvents(transportProvider, destination,
                events.stream().map(event -> createRecord(cluster, topicName, offsetIncrement.getAndIncrement(), partition, event, eventTimestamp, null))
                        .collect(Collectors.toList()),
                onComplete);
    }


    private void sendEvents(final BigqueryTransportProvider transportProvider, final String destination,
                            final List<DatastreamProducerRecord> records,
                            final SendCallback onComplete) {
        final CountDownLatch latch = new CountDownLatch(records.size());
        records.stream().forEachOrdered(record -> transportProvider.send(destination, record, ((metadata, exception) -> {
                    if (onComplete != null) {
                        onComplete.onCompletion(metadata, exception);
                    }
                    latch.countDown();
                })));
        try {
            while (!latch.await(500, TimeUnit.MILLISECONDS)) {
                transportProvider.flush();
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private final AtomicLong offsetIncrement = new AtomicLong();

    private DatastreamProducerRecord createRecord(final String cluster, final String topicName,
                                                  final long offset, final int partition, final GenericRecord event,
                                                  final DateTime eventTimestamp, final String sourceCheckpoint) {
        final DatastreamProducerRecordBuilder builder = new DatastreamProducerRecordBuilder();
        builder.setEventsSourceTimestamp(eventTimestamp.getValue());
        if (sourceCheckpoint != null) {
            builder.setSourceCheckpoint(sourceCheckpoint);
        }

        final BrooklinEnvelope envelope = new BrooklinEnvelope(null, serializer.serialize(topicName, event), null, ImmutableMap.of(
                KAFKA_ORIGIN_CLUSTER, cluster,
                KAFKA_ORIGIN_TOPIC, topicName,
                KAFKA_ORIGIN_PARTITION, String.valueOf(partition),
                KAFKA_ORIGIN_OFFSET, String.valueOf(offset),
                BrooklinEnvelopeMetadataConstants.EVENT_TIMESTAMP, String.valueOf(eventTimestamp.getValue())
        ));
        builder.addEvent(envelope);
        return builder.build();
    }

    private String getUniqueDatasetName() {
        return "testDataset_" + DATASET_COUNTER.incrementAndGet();
    }

    private String getUniqueTopicName() {
        return "testTopic_" + TOPIC_COUNTER.incrementAndGet();
    }
}
