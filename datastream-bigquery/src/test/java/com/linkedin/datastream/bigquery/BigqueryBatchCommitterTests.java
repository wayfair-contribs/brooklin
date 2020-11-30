package com.linkedin.datastream.bigquery;

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
import com.linkedin.datastream.bigquery.schema.SimpleBigquerySchemaEvolver;
import com.linkedin.datastream.common.DatastreamRecordMetadata;
import com.linkedin.datastream.common.DatastreamTransientException;
import com.linkedin.datastream.common.SendCallback;
import com.linkedin.datastream.metrics.DynamicMetricsManager;
import com.linkedin.datastream.server.api.transport.buffered.CommitCallback;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;
import org.mockito.stubbing.OngoingStubbing;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class BigqueryBatchCommitterTests {

    @BeforeClass
    public static void beforeClass() {
        DynamicMetricsManager.createInstance(new MetricRegistry(), BigqueryBatchCommitterTests.class.getSimpleName());
    }

    private BigQuery bigQuery;
    private BigqueryBatchCommitter batchCommitter;
    private BigquerySchemaEvolver schemaEvolver;

    @BeforeMethod
    void beforeTest() {
        bigQuery = mock(BigQuery.class);
        schemaEvolver = new SimpleBigquerySchemaEvolver();
        batchCommitter = new BigqueryBatchCommitter(bigQuery, 1);
    }

    @Test
    public void testCreateTableOnCommit() throws InterruptedException {
        final TableId tableId = TableId.of("dataset_name", "table_name");
        final long retention = -1L;
        final String destination = String.join("/", tableId.getDataset(), tableId.getTable(), Long.toString(retention));
        final Schema schema = Schema.of(
                Field.of("string", StandardSQLTypeName.STRING),
                Field.of("int", StandardSQLTypeName.INT64)
        );
        batchCommitter.setDestTableSchemaEvolver(destination, schemaEvolver);
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
        final TableId insertTableId = TableId.of(tableId.getDataset(),
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
        final TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
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
        batchCommitter.commit(rowsToInsert, destination, callbacks, metadata, timestamps, commitCallback);

        latch.await(1, TimeUnit.SECONDS);

        verify(bigQuery).getTable(tableId);
        verify(bigQuery).create(tableInfo);
        verify(bigQuery, times(2)).insertAll(insertAllRequest);
        verify(mockedRowCallback).onCompletion(metadata.get(0), null);
    }

    @Test
    public void testCreateTableFailure() throws InterruptedException {
        final TableId tableId = TableId.of("dataset_name", "table_name");
        final long retention = -1L;
        final String destination = String.join("/", tableId.getDataset(), tableId.getTable(), Long.toString(retention));
        final Schema schema = Schema.of(
                Field.of("string", StandardSQLTypeName.STRING),
                Field.of("int", StandardSQLTypeName.INT64)
        );
        batchCommitter.setDestTableSchemaEvolver(destination, schemaEvolver);
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
        batchCommitter.commit(rowsToInsert, destination, ImmutableList.of(sendCallback), ImmutableList.of(metadata),
                ImmutableList.of(), commitCallback);
        latch.await(1, TimeUnit.SECONDS);

        verify(bigQuery).getTable(tableId);
        verify(bigQuery).insertAll(any(InsertAllRequest.class));

        final ArgumentCaptor<Exception> exceptionArgumentCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(sendCallback).onCompletion(eq(metadata), exceptionArgumentCaptor.capture());
        final Exception actualException = exceptionArgumentCaptor.getValue();
        assertTrue(actualException instanceof DatastreamTransientException);
        assertEquals(actualException.getCause(), exception);
    }

    @Test
    public void testEvolveTableSchemaOnCommit() throws InterruptedException {
        final TableId tableId = TableId.of("dataset_name", "table_name");
        final long retention = -1L;
        final String destination = String.join("/", tableId.getDataset(), tableId.getTable(), Long.toString(retention));
        final Schema newSchema = Schema.of(
                Field.of("string", StandardSQLTypeName.STRING),
                Field.of("int", StandardSQLTypeName.INT64),
                Field.of("new_string", StandardSQLTypeName.STRING)
        );
        batchCommitter.setDestTableSchemaEvolver(destination, schemaEvolver);
        batchCommitter.setDestTableSchema(destination, newSchema);

        final Schema existingSchema = Schema.of(
                Field.of("string", StandardSQLTypeName.STRING),
                Field.of("int", StandardSQLTypeName.INT64)
        );
        final TableDefinition existingTableDefinition = StandardTableDefinition.of(existingSchema);
        final Table existingTable = mock(Table.class);

        when(existingTable.getDefinition()).thenReturn(existingTableDefinition);

        final Schema evolvedSchema = schemaEvolver.evolveSchema(existingSchema, newSchema);
        final TableDefinition evolvedTableDefinition = StandardTableDefinition.newBuilder()
                .setSchema(evolvedSchema)
                .setTimePartitioning(TimePartitioning.of(TimePartitioning.Type.DAY))
                .build();
        final Table.Builder tableBuilder = mock(Table.Builder.class);
        when(existingTable.toBuilder()).thenReturn(tableBuilder);
        when(tableBuilder.setDefinition(evolvedTableDefinition)).thenReturn(tableBuilder);

        final Table evolvedTable = mock(Table.class);
        when(tableBuilder.build()).thenReturn(evolvedTable);

        final TableId insertTableId = TableId.of(tableId.getDataset(),
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


        batchCommitter.commit(rowsToInsert, destination, ImmutableList.of(), ImmutableList.of(
                new DatastreamRecordMetadata("test", "test", 0)
                ),
                ImmutableList.of(), commitCallback);

        latch.await(1, TimeUnit.SECONDS);

        verify(bigQuery).getTable(tableId);
        verify(bigQuery, never()).create(any(TableInfo.class));
        verify(tableBuilder).setDefinition(evolvedTableDefinition);
        verify(evolvedTable).update();

        verify(bigQuery, times(2)).insertAll(insertAllRequest);
    }

    @Test
    public void testEvolveTableSchemaFailure() throws InterruptedException {
        final TableId tableId = TableId.of("dataset_name", "table_name");
        final long retention = -1L;
        final String destination = String.join("/", tableId.getDataset(), tableId.getTable(), Long.toString(retention));
        final Schema newSchema = Schema.of(
                Field.of("string", StandardSQLTypeName.STRING),
                Field.of("int", StandardSQLTypeName.INT64),
                Field.of("new_string", StandardSQLTypeName.STRING)
        );
        batchCommitter.setDestTableSchemaEvolver(destination, schemaEvolver);
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

        batchCommitter.commit(rowsToInsert, destination, ImmutableList.of(sendCallback), ImmutableList.of(metadata),
                ImmutableList.of(), commitCallback);

        latch.await(1, TimeUnit.SECONDS);

        verify(bigQuery, times(2)).getTable(tableId);
        verify(bigQuery, never()).create(any(TableInfo.class));
        verify(tableBuilder).setDefinition(evolvedTableDefinition);
        verify(bigQuery).insertAll(any(InsertAllRequest.class));

        final ArgumentCaptor<Exception> exceptionArgumentCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(sendCallback).onCompletion(eq(metadata), exceptionArgumentCaptor.capture());
        final Exception actualException = exceptionArgumentCaptor.getValue();
        assertTrue(actualException instanceof DatastreamTransientException);
        assertEquals(actualException.getCause(), exception);
    }


    @Test
    public void testEvolveTableSchemaConcurrencyFailure() throws InterruptedException {
        final TableId tableId = TableId.of("dataset_name", "table_name");
        final long retention = -1L;
        final String destination = String.join("/", tableId.getDataset(), tableId.getTable(), Long.toString(retention));
        final Schema newSchema = Schema.of(
                Field.of("string", StandardSQLTypeName.STRING),
                Field.of("int", StandardSQLTypeName.INT64),
                Field.of("new_string", StandardSQLTypeName.STRING)
        );
        batchCommitter.setDestTableSchemaEvolver(destination, schemaEvolver);
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
        final TableId insertTableId = TableId.of(tableId.getDataset(),
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

        batchCommitter.commit(rowsToInsert, destination, ImmutableList.of(), ImmutableList.of(
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

        verify(bigQuery, times(2)).insertAll(insertAllRequest);
    }
}
