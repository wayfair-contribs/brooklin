/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.bigquery;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.codahale.metrics.MetricRegistry;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import com.linkedin.datastream.bigquery.schema.BigquerySchemaEvolver;
import com.linkedin.datastream.bigquery.schema.BigquerySchemaEvolverFactory;
import com.linkedin.datastream.bigquery.schema.BigquerySchemaEvolverType;
import com.linkedin.datastream.bigquery.schema.SimpleBigquerySchemaEvolver;
import com.linkedin.datastream.common.DatastreamRecordMetadata;
import com.linkedin.datastream.common.SendCallback;
import com.linkedin.datastream.metrics.DynamicMetricsManager;
import com.linkedin.datastream.server.api.transport.buffered.CommitCallback;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class BigqueryBatchCommitterTests {

    @BeforeClass
    public static void beforeClass() {
        DynamicMetricsManager.createInstance(new MetricRegistry(), BigqueryBatchCommitterTests.class.getSimpleName());
    }

    private BigQuery bigQuery;
    private BigqueryBatchCommitter batchCommitter;
    private BigquerySchemaEvolver schemaEvolver;
    private Map<BigqueryDatastreamDestination, BigqueryDatastreamConfiguration> destinationConfiguraitons;

    @BeforeMethod
    void beforeTest() {
        bigQuery = mock(BigQuery.class);
        schemaEvolver = BigquerySchemaEvolverFactory.createBigquerySchemaEvolver(BigquerySchemaEvolverType.simple);
        destinationConfiguraitons = new HashMap<>();
        batchCommitter = new BigqueryBatchCommitter(bigQuery, 1, destinationConfiguraitons);
    }

    @Test
    public void testCreateTableOnCommit() throws InterruptedException {
        final TableId tableId = TableId.of("project_name", "dataset_name", "table_name");
        final BigqueryDatastreamDestination destination = new BigqueryDatastreamDestination(tableId.getProject(), tableId.getDataset(), tableId.getTable());
        final List<BigqueryLabel> labels = Arrays.asList(BigqueryLabel.of("test"), BigqueryLabel.of("name", "value"));
        destinationConfiguraitons.put(destination, new BigqueryDatastreamConfiguration(schemaEvolver, true, null, null, null, labels));
        final Schema schema = Schema.of(
                Field.of("string", StandardSQLTypeName.STRING),
                Field.of("int", StandardSQLTypeName.INT64)
        );
        batchCommitter.setDestTableSchema(destination, schema);

        when(bigQuery.getTable(tableId)).thenReturn(null);

        final CommitCallback commitCallback = mock(CommitCallback.class);
        final CountDownLatch latch = new CountDownLatch(1);
        doAnswer(invocation -> {
            latch.countDown();
            return null;
        }).when(commitCallback).commited();

        final ImmutableList<InsertAllRequest.RowToInsert> rowsToInsert = ImmutableList.of(
                InsertAllRequest.RowToInsert.of(ImmutableMap.of(
                        "string", "test",
                        "int", 123
                ))
        );
        final TableId insertTableId = TableId.of(tableId.getProject(), tableId.getDataset(),
                String.format("%s$%s", tableId.getTable(), LocalDate.now(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern("yyyyMMdd"))));
        final InsertAllRequest insertAllRequest = InsertAllRequest.of(insertTableId, rowsToInsert);
        final BigQueryException bigqueryException = new BigQueryException(404, "Table not found", new BigQueryError("notFound", null, "Table not found"));
        final AtomicBoolean tableCreated = new AtomicBoolean(false);
        when(bigQuery.insertAll(insertAllRequest)).then(invocation -> {
            if (tableCreated.get()) {
                return mock(InsertAllResponse.class);
            } else {
                throw bigqueryException;
            }
        });

        final TableDefinition tableDefinition = StandardTableDefinition.newBuilder().setSchema(schema)
                .setTimePartitioning(TimePartitioning.of(TimePartitioning.Type.DAY))
                .build();
        final TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition)
                .setLabels(labels.stream().collect(Collectors.toMap(BigqueryLabel::getName, BigqueryLabel::getValue)))
                .build();
        when(bigQuery.create(tableInfo)).then(invocation -> {
            if (!tableCreated.get()) {
                tableCreated.set(true);
                return mock(Table.class);
            } else {
                throw new IllegalStateException("Table already created");
            }
        });

        final SendCallback mockedRowCallback = mock(SendCallback.class);
        final ImmutableList<SendCallback> callbacks = ImmutableList.of(mockedRowCallback);
        final ImmutableList<DatastreamRecordMetadata> metadata = ImmutableList.of(
                new DatastreamRecordMetadata("test", "test", 0)
        );
        final ImmutableList<Long> timestamps = ImmutableList.of(System.currentTimeMillis());
        batchCommitter.commit(rowsToInsert, destination.toString(), callbacks, metadata, timestamps, commitCallback);

        latch.await(1, TimeUnit.SECONDS);

        verify(bigQuery).getTable(tableId);
        verify(bigQuery).create(tableInfo);
        verify(bigQuery).insertAll(insertAllRequest);
        verify(mockedRowCallback).onCompletion(metadata.get(0), null);
    }

    @Test
    public void testCreateTableFailure() throws InterruptedException {
        final TableId tableId = TableId.of("project_name", "dataset_name", "table_name");
        final BigqueryDatastreamDestination destination = new BigqueryDatastreamDestination(tableId.getProject(), tableId.getDataset(), tableId.getTable());
        destinationConfiguraitons.put(destination, new BigqueryDatastreamConfiguration(schemaEvolver, true));
        final Schema schema = Schema.of(
                Field.of("string", StandardSQLTypeName.STRING),
                Field.of("int", StandardSQLTypeName.INT64)
        );
        batchCommitter.setDestTableSchema(destination, schema);

        when(bigQuery.getTable(tableId)).thenReturn(null);

        final CommitCallback commitCallback = mock(CommitCallback.class);
        final CountDownLatch latch = new CountDownLatch(1);
        doAnswer(invocation -> {
            latch.countDown();
            return null;
        }).when(commitCallback).commited();

        final ImmutableList<InsertAllRequest.RowToInsert> rowsToInsert = ImmutableList.of(
                InsertAllRequest.RowToInsert.of(ImmutableMap.of(
                        "string", "test",
                        "int", 123
                ))
        );

        final TableDefinition tableDefinition = StandardTableDefinition.newBuilder().setSchema(schema)
                .setTimePartitioning(TimePartitioning.of(TimePartitioning.Type.DAY))
                .build();
        final TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();

        final BigQueryException exception = new BigQueryException(400, "Test create table failure");
        when(bigQuery.create(tableInfo)).thenThrow(exception);
        final SendCallback sendCallback = mock(SendCallback.class);
        final DatastreamRecordMetadata metadata = new DatastreamRecordMetadata("test", "test", 0);
        batchCommitter.commit(rowsToInsert, destination.toString(), ImmutableList.of(sendCallback), ImmutableList.of(metadata),
                ImmutableList.of(), commitCallback);
        latch.await(1, TimeUnit.SECONDS);

        verify(bigQuery, times(2)).getTable(tableId);
        verify(bigQuery).insertAll(any(InsertAllRequest.class));

        final ArgumentCaptor<Exception> exceptionArgumentCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(sendCallback).onCompletion(eq(metadata), exceptionArgumentCaptor.capture());
        final Exception actualException = exceptionArgumentCaptor.getValue();
        assertEquals(actualException, exception);
    }


    @Test
    public void testTableNotCreatedWhenNotManaged() throws InterruptedException {
        final TableId tableId = TableId.of("project_name", "dataset_name", "table_name");
        final BigqueryDatastreamDestination destination = new BigqueryDatastreamDestination(tableId.getProject(), tableId.getDataset(), tableId.getTable());
        destinationConfiguraitons.put(destination, new BigqueryDatastreamConfiguration(schemaEvolver, false));
        final Schema schema = Schema.of(
                Field.of("string", StandardSQLTypeName.STRING),
                Field.of("int", StandardSQLTypeName.INT64)
        );
        batchCommitter.setDestTableSchema(destination, schema);

        final CommitCallback commitCallback = mock(CommitCallback.class);
        final CountDownLatch latch = new CountDownLatch(1);
        doAnswer(invocation -> {
            latch.countDown();
            return null;
        }).when(commitCallback).commited();

        final ImmutableList<InsertAllRequest.RowToInsert> rowsToInsert = ImmutableList.of(
                InsertAllRequest.RowToInsert.of(ImmutableMap.of(
                        "string", "test",
                        "int", 123
                ))
        );
        final TableId insertTableId = TableId.of(tableId.getProject(), tableId.getDataset(),
                String.format("%s$%s", tableId.getTable(), LocalDate.now(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern("yyyyMMdd"))));
        final InsertAllRequest insertAllRequest = InsertAllRequest.of(insertTableId, rowsToInsert);
        final BigQueryException bigqueryException = new BigQueryException(404, "Table not found", new BigQueryError("notFound", null, "Table not found"));
        when(bigQuery.insertAll(insertAllRequest)).thenThrow(bigqueryException);

        final List<DatastreamRecordMetadata> capturedRecordMetadata = new LinkedList<>();
        final List<Exception> capturedExceptions = new LinkedList<>();

        final SendCallback rowCallback = ((metadata, exception) -> {
            capturedRecordMetadata.add(metadata);
            Optional.ofNullable(exception).ifPresent(capturedExceptions::add);
        });
        final ImmutableList<SendCallback> callbacks = ImmutableList.of(rowCallback);
        final ImmutableList<DatastreamRecordMetadata> metadata = ImmutableList.of(
                new DatastreamRecordMetadata("test", "test", 0)
        );
        final ImmutableList<Long> timestamps = ImmutableList.of(System.currentTimeMillis());
        batchCommitter.commit(rowsToInsert, destination.toString(), callbacks, metadata, timestamps, commitCallback);

        latch.await(1, TimeUnit.SECONDS);

        verify(bigQuery, never()).getTable(any(TableId.class));
        verify(bigQuery, never()).create(any(TableInfo.class));
        verify(bigQuery, atLeastOnce()).insertAll(insertAllRequest);
        assertEquals(capturedRecordMetadata, metadata);
        assertFalse(capturedExceptions.isEmpty());
        capturedExceptions.forEach(e -> {
            assertTrue(e instanceof TransientStreamingInsertException);
            assertEquals(e.getCause(), bigqueryException);
        });
    }

    @Test
    public void testEvolveTableSchemaOnCommit() throws InterruptedException {
        final TableId tableId = TableId.of("project_name", "dataset_name", "table_name");
        final BigqueryDatastreamDestination destination = new BigqueryDatastreamDestination(tableId.getProject(), tableId.getDataset(), tableId.getTable());
        final List<BigqueryLabel> labels = Arrays.asList(BigqueryLabel.of("test"), BigqueryLabel.of("name", "value"));
        destinationConfiguraitons.put(destination, new BigqueryDatastreamConfiguration(schemaEvolver, true, null, null, null, labels));
        final Schema newSchema = Schema.of(
                Field.of("string", StandardSQLTypeName.STRING),
                Field.of("int", StandardSQLTypeName.INT64),
                Field.of("new_string", StandardSQLTypeName.STRING)
        );
        batchCommitter.setDestTableSchema(destination, newSchema);

        final Schema existingSchema = Schema.of(
                Field.of("string", StandardSQLTypeName.STRING),
                Field.of("int", StandardSQLTypeName.INT64)
        );
        final TableDefinition existingTableDefinition = StandardTableDefinition.of(existingSchema);
        final Table existingTable = mock(Table.class);

        when(existingTable.getDefinition()).thenReturn(existingTableDefinition);
        when(existingTable.getLabels()).thenReturn(Collections.emptyMap());

        final Schema evolvedSchema = schemaEvolver.evolveSchema(existingSchema, newSchema);
        final TableDefinition evolvedTableDefinition = StandardTableDefinition.newBuilder()
                .setSchema(evolvedSchema)
                .setTimePartitioning(TimePartitioning.of(TimePartitioning.Type.DAY))
                .build();
        final Table.Builder tableBuilder = mock(Table.Builder.class);
        when(existingTable.toBuilder()).thenReturn(tableBuilder);
        when(tableBuilder.setDefinition(evolvedTableDefinition)).thenReturn(tableBuilder);
        final Map<String, String> labelsMap = labels.stream().collect(Collectors.toMap(BigqueryLabel::getName, BigqueryLabel::getValue));
        when(tableBuilder.setLabels(labelsMap)).thenReturn(tableBuilder);

        final Table evolvedTable = mock(Table.class);
        when(tableBuilder.build()).thenReturn(evolvedTable);

        final TableId insertTableId = TableId.of(tableId.getProject(), tableId.getDataset(),
                String.format("%s$%s", tableId.getTable(), LocalDate.now(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern("yyyyMMdd"))));
        final ImmutableList<InsertAllRequest.RowToInsert> rowsToInsert = ImmutableList.of(
                InsertAllRequest.RowToInsert.of(ImmutableMap.of(
                        "string", "test value 1",
                        "int", 123,
                        "new_string", "test value 2"
                ))
        );
        final InsertAllRequest insertAllRequest = InsertAllRequest.of(insertTableId, rowsToInsert);
        final AtomicBoolean tableUpdated = new AtomicBoolean(false);
        when(evolvedTable.update()).then(invocation -> {
            if (!tableUpdated.get()) {
                tableUpdated.set(true);
                return mock(Table.class);
            } else {
                throw new IllegalStateException("Table already updated");
            }
        });
        when(bigQuery.insertAll(insertAllRequest)).then(invocation -> {
            if (tableUpdated.get()) {
                return mock(InsertAllResponse.class);
            } else {
                throw new BigQueryException(400, "Missing column", new BigQueryError("invalid", "new_string", "Missing column"));
            }
        });

        when(bigQuery.getTable(tableId)).thenReturn(existingTable);

        final CommitCallback commitCallback = mock(CommitCallback.class);
        final CountDownLatch latch = new CountDownLatch(1);
        doAnswer(invocation -> {
            latch.countDown();
            return null;
        }).when(commitCallback).commited();


        batchCommitter.commit(rowsToInsert, destination.toString(), ImmutableList.of(), ImmutableList.of(
                new DatastreamRecordMetadata("test", "test", 0)
                ),
                ImmutableList.of(), commitCallback);

        latch.await(1, TimeUnit.SECONDS);

        verify(bigQuery).getTable(tableId);
        verify(bigQuery, never()).create(any(TableInfo.class));
        verify(tableBuilder).setDefinition(evolvedTableDefinition);
        verify(tableBuilder).setLabels(labelsMap);
        verify(evolvedTable).update();

        verify(bigQuery).insertAll(insertAllRequest);
    }

    @Test
    public void testSchemaNotEvolvedWhenNotManaged() throws InterruptedException {
        final TableId tableId = TableId.of("project_name", "dataset_name", "table_name");
        final BigqueryDatastreamDestination destination = new BigqueryDatastreamDestination(tableId.getProject(), tableId.getDataset(), tableId.getTable());
        destinationConfiguraitons.put(destination, new BigqueryDatastreamConfiguration(schemaEvolver, false));

        final TableId insertTableId = TableId.of(tableId.getProject(), tableId.getDataset(),
                String.format("%s$%s", tableId.getTable(), LocalDate.now(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern("yyyyMMdd"))));
        final ImmutableList<InsertAllRequest.RowToInsert> rowsToInsert = ImmutableList.of(
                InsertAllRequest.RowToInsert.of(ImmutableMap.of(
                        "string", "test value 1",
                        "int", 123,
                        "missing", "test value 2"
                ))
        );
        final InsertAllRequest insertAllRequest = InsertAllRequest.of(insertTableId, rowsToInsert);

        final InsertAllResponse response = mock(InsertAllResponse.class);
        final Map<Long, List<BigQueryError>> insertErrors = ImmutableMap.of(0L, ImmutableList.of(new BigQueryError("invalid", "missing", "Missing column")));
        when(response.getInsertErrors()).thenReturn(insertErrors);
        when(response.hasErrors()).thenReturn(!insertErrors.isEmpty());
        when(bigQuery.insertAll(insertAllRequest)).thenReturn(response);

        final CommitCallback commitCallback = mock(CommitCallback.class);
        final CountDownLatch latch = new CountDownLatch(1);
        doAnswer(invocation -> {
            latch.countDown();
            return null;
        }).when(commitCallback).commited();

        final List<DatastreamRecordMetadata> capturedRecordMetadata = new LinkedList<>();
        final List<Exception> capturedExceptions = new LinkedList<>();
        final SendCallback rowCallback = ((metadata, exception) -> {
            capturedRecordMetadata.add(metadata);
            Optional.ofNullable(exception).ifPresent(capturedExceptions::add);
        });
        final ImmutableList<SendCallback> callbacks = ImmutableList.of(rowCallback);
        final ImmutableList<DatastreamRecordMetadata> metadata = ImmutableList.of(new DatastreamRecordMetadata("test", "test", 0));
        final ImmutableList<Long> timestamps = ImmutableList.of(System.currentTimeMillis());
        batchCommitter.commit(rowsToInsert, destination.toString(), callbacks, metadata, timestamps, commitCallback);

        latch.await(1, TimeUnit.SECONDS);

        verify(bigQuery, never()).getTable(tableId);
        verify(bigQuery, never()).create(any(TableInfo.class));
        verify(bigQuery, atLeastOnce()).insertAll(any(InsertAllRequest.class));
        assertEquals(capturedRecordMetadata, metadata);
        assertFalse(capturedExceptions.isEmpty());
        capturedExceptions.forEach(e -> {
            assertTrue(e instanceof TransientStreamingInsertException);
            assertEquals(e.getMessage(), ImmutableList.of(new BigQueryError("invalid", "missing", "Missing column")).toString());
        });
    }

    @Test
    public void testEvolveTableSchemaFailure() throws InterruptedException {
        final TableId tableId = TableId.of("project_name", "dataset_name", "table_name");
        final BigqueryDatastreamDestination destination = new BigqueryDatastreamDestination(tableId.getProject(), tableId.getDataset(), tableId.getTable());
        destinationConfiguraitons.put(destination, new BigqueryDatastreamConfiguration(schemaEvolver, true));
        final Schema newSchema = Schema.of(
                Field.of("string", StandardSQLTypeName.STRING),
                Field.of("int", StandardSQLTypeName.INT64),
                Field.of("new_string", StandardSQLTypeName.STRING)
        );
        batchCommitter.setDestTableSchema(destination, newSchema);

        final Schema existingSchema = Schema.of(
                Field.of("string", StandardSQLTypeName.STRING),
                Field.of("int", StandardSQLTypeName.INT64)
        );
        final TableDefinition existingTableDefinition = StandardTableDefinition.of(existingSchema);
        final Table existingTable = mock(Table.class);

        when(existingTable.getDefinition()).thenReturn(existingTableDefinition);

        final Table.Builder tableBuilder = mock(Table.Builder.class);
        final Schema evolvedSchema = schemaEvolver.evolveSchema(existingSchema, newSchema);
        final TableDefinition evolvedTableDefinition = StandardTableDefinition.newBuilder()
                .setSchema(evolvedSchema)
                .setTimePartitioning(TimePartitioning.of(TimePartitioning.Type.DAY))
                .build();
        when(tableBuilder.setDefinition(evolvedTableDefinition)).thenReturn(tableBuilder);
        final Table evolvedTable = mock(Table.class);
        when(tableBuilder.build()).thenReturn(evolvedTable);
        when(existingTable.toBuilder()).thenReturn(tableBuilder);

        when(bigQuery.getTable(tableId)).thenReturn(existingTable);

        final CommitCallback commitCallback = mock(CommitCallback.class);
        final CountDownLatch latch = new CountDownLatch(1);
        doAnswer(invocation -> {
            latch.countDown();
            return null;
        }).when(commitCallback).commited();

        final ImmutableList<InsertAllRequest.RowToInsert> rowsToInsert = ImmutableList.of(
                InsertAllRequest.RowToInsert.of(ImmutableMap.of(
                        "string", "test",
                        "int", 123
                ))
        );

        final BigQueryException exception = new BigQueryException(400, "Test update table failure");
        when(evolvedTable.update()).thenThrow(exception);

        final SendCallback sendCallback = mock(SendCallback.class);
        final DatastreamRecordMetadata metadata = new DatastreamRecordMetadata("test", "test", 0);

        batchCommitter.commit(rowsToInsert, destination.toString(), ImmutableList.of(sendCallback), ImmutableList.of(metadata),
                ImmutableList.of(), commitCallback);

        latch.await(1, TimeUnit.SECONDS);

        verify(bigQuery, times(4)).getTable(tableId);
        verify(bigQuery, never()).create(any(TableInfo.class));
        verify(tableBuilder, times(2)).setDefinition(evolvedTableDefinition);
        verify(bigQuery).insertAll(any(InsertAllRequest.class));

        final ArgumentCaptor<Exception> exceptionArgumentCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(sendCallback).onCompletion(eq(metadata), exceptionArgumentCaptor.capture());
        final Exception actualException = exceptionArgumentCaptor.getValue();
        assertEquals(actualException, exception);
    }


    @Test
    public void testEvolveTableSchemaConcurrencyFailure() throws InterruptedException {
        final TableId tableId = TableId.of("project_name", "dataset_name", "table_name");
        final BigqueryDatastreamDestination destination = new BigqueryDatastreamDestination(tableId.getProject(), tableId.getDataset(), tableId.getTable());
        destinationConfiguraitons.put(destination, new BigqueryDatastreamConfiguration(schemaEvolver, true));
        final Schema newSchema = Schema.of(
                Field.of("string", StandardSQLTypeName.STRING),
                Field.of("int", StandardSQLTypeName.INT64),
                Field.of("new_string", StandardSQLTypeName.STRING)
        );
        batchCommitter.setDestTableSchema(destination, newSchema);

        final Schema existingSchema = Schema.of(
                Field.of("string", StandardSQLTypeName.STRING),
                Field.of("int", StandardSQLTypeName.INT64)
        );
        final TableDefinition existingTableDefinition = StandardTableDefinition.of(existingSchema);
        final Table existingTable = mock(Table.class);

        when(existingTable.getDefinition()).thenReturn(existingTableDefinition);

        final Schema newBaseSchema = Schema.of(
                Field.of("string", StandardSQLTypeName.STRING),
                Field.of("int", StandardSQLTypeName.INT64),
                Field.newBuilder("new_int", StandardSQLTypeName.INT64).setMode(Field.Mode.NULLABLE).build()
        );
        final TableDefinition newBaseTableDefinition = StandardTableDefinition.of(newBaseSchema);
        final Table newBaseTable = mock(Table.class);

        when(newBaseTable.getDefinition()).thenReturn(newBaseTableDefinition);

        when(bigQuery.getTable(tableId)).thenReturn(existingTable).thenReturn(newBaseTable);

        final Table.Builder tableBuilder1 = mock(Table.Builder.class);
        final Schema evolvedSchema1 = schemaEvolver.evolveSchema(existingSchema, newSchema);
        final TableDefinition evolvedTableDefinition1 = StandardTableDefinition.newBuilder()
                .setSchema(evolvedSchema1)
                .setTimePartitioning(TimePartitioning.of(TimePartitioning.Type.DAY))
                .build();
        when(tableBuilder1.setDefinition(evolvedTableDefinition1)).thenReturn(tableBuilder1);
        final Table evolvedTable1 = mock(Table.class);
        when(tableBuilder1.build()).thenReturn(evolvedTable1);
        when(existingTable.toBuilder()).thenReturn(tableBuilder1);

        final BigQueryException exception = new BigQueryException(400, "Test update table failure");
        when(evolvedTable1.update()).thenThrow(exception);

        final Table.Builder tableBuilder2 = mock(Table.Builder.class);
        final Schema evolvedSchema2 = schemaEvolver.evolveSchema(newBaseSchema, newSchema);
        final TableDefinition evolvedTableDefinition2 = StandardTableDefinition.newBuilder()
                .setSchema(evolvedSchema2)
                .setTimePartitioning(TimePartitioning.of(TimePartitioning.Type.DAY))
                .build();
        when(tableBuilder2.setDefinition(evolvedTableDefinition2)).thenReturn(tableBuilder2);
        final Table evolvedTable2 = mock(Table.class);
        when(tableBuilder2.build()).thenReturn(evolvedTable2);
        when(newBaseTable.toBuilder()).thenReturn(tableBuilder2);


        final CommitCallback commitCallback = mock(CommitCallback.class);
        final CountDownLatch latch = new CountDownLatch(1);
        doAnswer(invocation -> {
            latch.countDown();
            return null;
        }).when(commitCallback).commited();

        final ImmutableList<InsertAllRequest.RowToInsert> rowsToInsert = ImmutableList.of(
                InsertAllRequest.RowToInsert.of(ImmutableMap.of(
                        "string", "test",
                        "int", 123
                ))
        );
        final TableId insertTableId = TableId.of(tableId.getProject(), tableId.getDataset(),
                String.format("%s$%s", tableId.getTable(), LocalDate.now(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern("yyyyMMdd"))));
        final InsertAllRequest insertAllRequest = InsertAllRequest.of(insertTableId, rowsToInsert);

        final AtomicBoolean tableUpdated = new AtomicBoolean(false);
        when(evolvedTable2.update()).then(invocation -> {
            if (!tableUpdated.get()) {
                tableUpdated.set(true);
                return mock(Table.class);
            } else {
                throw new IllegalStateException("Table already updated");
            }
        });

        when(bigQuery.insertAll(insertAllRequest)).then(invocation -> {
            if (tableUpdated.get()) {
                return mock(InsertAllResponse.class);
            } else {
                throw new BigQueryException(400, "Missing column", new BigQueryError("invalid", "new_string", "Missing column"));
            }
        });

        batchCommitter.commit(rowsToInsert, destination.toString(), ImmutableList.of(), ImmutableList.of(
                new DatastreamRecordMetadata("test", "test", 0)
                ),
                ImmutableList.of(), commitCallback);

        latch.await(1, TimeUnit.SECONDS);

        verify(bigQuery, times(2)).getTable(tableId);
        verify(bigQuery, never()).create(any(TableInfo.class));

        verify(tableBuilder1).setDefinition(evolvedTableDefinition1);
        verify(evolvedTable1).update();

        verify(tableBuilder2).setDefinition(evolvedTableDefinition2);
        verify(evolvedTable2).update();

        verify(bigQuery).insertAll(insertAllRequest);
    }

    @Test
    public void testPayloadSizeLimitRetry() {
        final TableId tableId = TableId.of("dataset", "table");
        final List<InsertAllRequest.RowToInsert> batch = IntStream.range(0, 10)
                .mapToObj(i -> InsertAllRequest.RowToInsert.of(ImmutableMap.of("f1", "test" + i)))
                .collect(Collectors.toList());
        final InsertAllResponse response = mock(InsertAllResponse.class);
        when(response.hasErrors()).thenReturn(false);
        when(response.getInsertErrors()).thenReturn(Collections.emptyMap());
        when(bigQuery.insertAll(any(InsertAllRequest.class)))
                .thenThrow(new BigQueryException(400, "Request payload size exceeds the limit"))
                .thenReturn(response);
        final Map<Integer, Exception> insertErrors = batchCommitter.insertRowsAndMapErrorsWithRetry(tableId, batch);
        verify(bigQuery).insertAll(InsertAllRequest.of(tableId, batch));
        verify(bigQuery).insertAll(InsertAllRequest.of(tableId, batch.subList(0, batch.size() / 2)));
        verify(bigQuery).insertAll(InsertAllRequest.of(tableId, batch.subList(batch.size() / 2, batch.size())));
        assertTrue(insertErrors.isEmpty());
    }

    @Test
    public void testBatchSizeLimitRetry() {
        final TableId tableId = TableId.of("dataset", "table");
        final List<InsertAllRequest.RowToInsert> batch = IntStream.range(0, 10)
                .mapToObj(i -> InsertAllRequest.RowToInsert.of(ImmutableMap.of("f1", "test" + i)))
                .collect(Collectors.toList());
        final InsertAllResponse response = mock(InsertAllResponse.class);
        when(response.hasErrors()).thenReturn(false);
        when(response.getInsertErrors()).thenReturn(Collections.emptyMap());
        when(bigQuery.insertAll(any(InsertAllRequest.class)))
                .thenThrow(new BigQueryException(400, "too many rows present in the request"))
                .thenReturn(response);
        final Map<Integer, Exception> insertErrors = batchCommitter.insertRowsAndMapErrorsWithRetry(tableId, batch);
        verify(bigQuery).insertAll(InsertAllRequest.of(tableId, batch));
        verify(bigQuery).insertAll(InsertAllRequest.of(tableId, batch.subList(0, batch.size() / 2)));
        verify(bigQuery).insertAll(InsertAllRequest.of(tableId, batch.subList(batch.size() / 2, batch.size())));
        assertTrue(insertErrors.isEmpty());
    }

    @Test
    public void testBatchSizeLimitRetryRemappedRowErrorIndex() {
        final TableId tableId = TableId.of("dataset", "table");
        final List<InsertAllRequest.RowToInsert> batch = IntStream.range(0, 10)
                .mapToObj(i -> InsertAllRequest.RowToInsert.of(ImmutableMap.of("f1", "test" + i)))
                .collect(Collectors.toList());
        final InsertAllResponse response = mock(InsertAllResponse.class);
        when(response.hasErrors()).thenReturn(false);
        when(response.getInsertErrors()).thenReturn(Collections.emptyMap());
        final BigQueryException batchException = new BigQueryException(400, "too many rows present in the request");
        when(bigQuery.insertAll(any(InsertAllRequest.class)))
                .thenThrow(batchException, batchException)
                .thenThrow(new RuntimeException("non-batch size limit exception"));
        final Map<Integer, Exception> insertErrors = batchCommitter.insertRowsAndMapErrorsWithRetry(tableId, batch);
        verify(bigQuery).insertAll(InsertAllRequest.of(tableId, batch));
        verify(bigQuery).insertAll(InsertAllRequest.of(tableId, batch.subList(0, batch.size() / 2)));
        verify(bigQuery).insertAll(InsertAllRequest.of(tableId, batch.subList(0, batch.size() / 4)));
        verify(bigQuery).insertAll(InsertAllRequest.of(tableId, batch.subList(batch.size() / 4, batch.size() / 2)));
        verify(bigQuery).insertAll(InsertAllRequest.of(tableId, batch.subList(batch.size() / 2, batch.size())));
        assertEquals(insertErrors.keySet(), IntStream.range(0, batch.size()).boxed().collect(Collectors.toSet()));
    }

    @Test
    public void testBatchSizeLimitRetryMultipleTimes() {
        final TableId tableId = TableId.of("dataset", "table");
        final List<InsertAllRequest.RowToInsert> batch = IntStream.range(0, 10)
                .mapToObj(i -> InsertAllRequest.RowToInsert.of(ImmutableMap.of("f1", "test" + i)))
                .collect(Collectors.toList());
        final InsertAllResponse response = mock(InsertAllResponse.class);
        when(response.hasErrors()).thenReturn(false);
        when(response.getInsertErrors()).thenReturn(Collections.emptyMap());
        final Exception exception = new BigQueryException(400, "too many rows present in the request");
        when(bigQuery.insertAll(any(InsertAllRequest.class)))
                .thenThrow(exception, exception)
                .thenReturn(response);
        final Map<Integer, Exception> insertErrors = batchCommitter.insertRowsAndMapErrorsWithRetry(tableId, batch);
        verify(bigQuery).insertAll(InsertAllRequest.of(tableId, batch));
        verify(bigQuery).insertAll(InsertAllRequest.of(tableId, batch.subList(0, batch.size() / 2)));
        verify(bigQuery).insertAll(InsertAllRequest.of(tableId, batch.subList(0, batch.size() / 4)));
        verify(bigQuery).insertAll(InsertAllRequest.of(tableId, batch.subList(batch.size() / 4, batch.size() / 2)));
        verify(bigQuery).insertAll(InsertAllRequest.of(tableId, batch.subList(batch.size() / 2, batch.size())));
        assertTrue(insertErrors.isEmpty());
    }

    @Test
    public void testBatchSizeLimitRetryInfiniteRecursionExit() {
        final TableId tableId = TableId.of("dataset", "table");
        final List<InsertAllRequest.RowToInsert> batch = IntStream.range(0, 10)
                .mapToObj(i -> InsertAllRequest.RowToInsert.of(ImmutableMap.of("f1", "test" + i)))
                .collect(Collectors.toList());
        final InsertAllResponse response = mock(InsertAllResponse.class);
        when(response.hasErrors()).thenReturn(false);
        when(response.getInsertErrors()).thenReturn(Collections.emptyMap());
        final Exception exception = new BigQueryException(400, "too many rows present in the request");
        when(bigQuery.insertAll(any(InsertAllRequest.class)))
                .thenThrow(exception);
        final Map<Integer, Exception> insertErrors = batchCommitter.insertRowsAndMapErrorsWithRetry(tableId, batch);
        verify(bigQuery, times(19)).insertAll(any(InsertAllRequest.class));
        assertEquals(insertErrors.keySet(), IntStream.range(0, batch.size()).boxed().collect(Collectors.toSet()));
        insertErrors.values().stream().map(Exception::getCause).forEach(e -> assertEquals(e, exception));
    }

}
