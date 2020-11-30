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
import com.linkedin.datastream.bigquery.schema.BigquerySchemaEvolver;
import com.linkedin.datastream.bigquery.schema.SimpleBigquerySchemaEvolver;
import com.linkedin.datastream.bigquery.translator.RecordTranslator;
import com.linkedin.datastream.bigquery.translator.SchemaTranslator;
import com.linkedin.datastream.common.BrooklinEnvelope;
import com.linkedin.datastream.common.PollUtils;
import com.linkedin.datastream.metrics.DynamicMetricsManager;
import com.linkedin.datastream.server.DatastreamProducerRecord;
import com.linkedin.datastream.server.DatastreamProducerRecordBuilder;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.zookeeper.Environment;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

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

        final BigqueryBatchCommitter committer = new BigqueryBatchCommitter(bigQuery, 1);
        final BatchBuilder batchBuilder = new BatchBuilder(
                maxBatchSize, maxBatchAge, maxInflightCommits, committer, queueSize, schemaRegistry, schemaEvolvers,
                defaultSchemaEvolverName
        );
        final List<BatchBuilder> batchBuilders = ImmutableList.of(batchBuilder);

        final String datasetName = getUniqueDatasetName();
        final long retention = -1;
        final String destination = String.format("%s/%s", datasetName, retention);
        final String topicName = getUniqueTopicName();

        final TableId tableId = TableId.of(datasetName, topicName);
        final TableId insertTableId = TableId.of(tableId.getDataset(),
                String.format("%s$%s", tableId.getTable(), LocalDate.now(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern("yyyyMMdd"))));

        final InsertAllResponse insertAllResponse = mock(InsertAllResponse.class);
        when(insertAllResponse.getInsertErrors()).thenReturn(ImmutableMap.of());
        when(bigQuery.insertAll(any(InsertAllRequest.class))).thenReturn(insertAllResponse);

        final DateTime eventTimestamp = new DateTime(new Date(System.currentTimeMillis()));
        try (final BigqueryTransportProvider transportProvider = new BigqueryTransportProvider("test", committer, batchBuilders, maxBatchAge)) {
            sendEvents(transportProvider, destination, topicName, 0, events, eventTimestamp);
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

    private static InsertAllRequest getExpectedRequest(final InsertAllRequest actualRequest, final TableId insertTableId, final List<GenericRecord> expectedRecords) {
        assertEquals(actualRequest.getRows().size(), expectedRecords.size());
        return InsertAllRequest.of(insertTableId, IntStream.range(0, expectedRecords.size())
                .mapToObj(index -> getExpectedRowToInsert(actualRequest.getRows().get(index), expectedRecords.get(index)))
                .collect(Collectors.toList()));
    }

    @Test
    public void testSendAndEvolveSchema() throws IOException, RestClientException {
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

        final BigqueryBatchCommitter committer = new BigqueryBatchCommitter(bigQuery, 1);

        final BatchBuilder batchBuilder = new BatchBuilder(
                maxBatchSize, maxBatchAge, maxInflightCommits, committer, queueSize, schemaRegistry, schemaEvolvers, defaultSchemaEvolverName
        );
        final List<BatchBuilder> batchBuilders = ImmutableList.of(batchBuilder);

        final String datasetName = getUniqueDatasetName();
        final long retention = -1;
        final String destination = String.format("%s/%s", datasetName, retention);
        final String topicName = getUniqueTopicName();

        final TableId tableId = TableId.of(datasetName, topicName);
        final TableId insertTableId = TableId.of(tableId.getDataset(),
                String.format("%s$%s", tableId.getTable(), LocalDate.now(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern("yyyyMMdd"))));

        final com.google.cloud.bigquery.Schema firstSchema = SchemaTranslator.translate(schema1);
        final TableDefinition tableDefinition = StandardTableDefinition.newBuilder()
                .setSchema(firstSchema)
                .setTimePartitioning(TimePartitioning.of(TimePartitioning.Type.DAY))
                .build();
        final TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();

        final Table mockedTable = mock(Table.class);
        when(mockedTable.getDefinition()).thenReturn(tableDefinition);

        final Table.Builder tableBuilder = mock(Table.Builder.class);
        when(mockedTable.toBuilder()).thenReturn(tableBuilder);
        final TableDefinition evolvedTableDefinition = StandardTableDefinition.newBuilder()
                .setSchema(schemaEvolvers.get(defaultSchemaEvolverName).evolveSchema(firstSchema, SchemaTranslator.translate(schema2)))
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
                    //request.getRows().stream().map(row -> row.getContent().entrySet().stream()
                    //                        .filter(entry -> !"__metadata".equals(entry.getKey()))
                    //                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
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

        try (final BigqueryTransportProvider transportProvider = new BigqueryTransportProvider("test", committer, batchBuilders, maxBatchAge)) {
            final DateTime eventTimestamp1 = new DateTime(new Date(System.currentTimeMillis()));
            sendEvents(transportProvider, destination, topicName, 0, events1, eventTimestamp1);

            verify(bigQuery, atLeastOnce()).insertAll(any(InsertAllRequest.class));

            final DateTime eventTimestamp2 = new DateTime(new Date(System.currentTimeMillis()));
            sendEvents(transportProvider, destination, topicName, 0, events2, eventTimestamp2);

            verify(evolvedTable).update();
            verify(bigQuery, atLeastOnce()).insertAll(any(InsertAllRequest.class));

            assertEquals(insertedData, Stream.concat(data1.stream(), data2.stream()).collect(Collectors.toList()));
        }
    }

    private static final String schemaNameSuffix = "-value";

    private SchemaRegistry schemaRegistry;
    private KafkaAvroSerializer serializer;
    private BigQuery bigQuery;
    private Map<String, BigquerySchemaEvolver> schemaEvolvers;
    private String defaultSchemaEvolverName;

    @BeforeMethod
    void beforeTest() {
        final MockSchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
        final String schemaRegistryUrl = "http://schema-registry/";
        schemaRegistry = new SchemaRegistry(schemaRegistryUrl, schemaRegistryClient, schemaNameSuffix);
        serializer = new KafkaAvroSerializer(schemaRegistryClient, ImmutableMap.of(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl));
        bigQuery = mock(BigQuery.class);
        defaultSchemaEvolverName = "simple";
        schemaEvolvers = Collections.singletonMap(defaultSchemaEvolverName, new SimpleBigquerySchemaEvolver());
    }

    private void sendEvents(final BigqueryTransportProvider transportProvider, final String destination, final String topicName,
                            final int partition, final List<GenericRecord> events, final DateTime eventTimestamp) {
        final AtomicInteger callbacksCalledCount = new AtomicInteger();
        events.stream().map(event -> createRecord(topicName, offsetIncrement.getAndIncrement(), partition, event, eventTimestamp))
                .forEachOrdered(record -> transportProvider.send(destination, record, ((metadata, exception) -> callbacksCalledCount.incrementAndGet())));
        transportProvider.flush();
        assertTrue(PollUtils.poll(() -> callbacksCalledCount.intValue() == events.size(), 1000, 10000),
                "Send callback was not called; likely topic was not created in time");
    }

    private final AtomicLong offsetIncrement = new AtomicLong();

    private DatastreamProducerRecord createRecord(final String topicName,
                                                  final long offset, final int partition, final GenericRecord event,
                                                  final DateTime eventTimestamp) {
        final DatastreamProducerRecordBuilder builder = new DatastreamProducerRecordBuilder();
        builder.setEventsSourceTimestamp(eventTimestamp.getValue());
        final String KAFKA_ORIGIN_TOPIC = "kafka-origin-topic";
        final String KAFKA_ORIGIN_PARTITION = "kafka-origin-partition";
        final String KAFKA_ORIGIN_OFFSET = "kafka-origin-offset";
        final BrooklinEnvelope envelope = new BrooklinEnvelope(null, serializer.serialize(topicName, event), null, ImmutableMap.of(
                KAFKA_ORIGIN_TOPIC, topicName,
                KAFKA_ORIGIN_PARTITION, String.valueOf(partition),
                KAFKA_ORIGIN_OFFSET, String.valueOf(offset)
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
