/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.bigquery;

import java.nio.BufferUnderflowException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.errors.SerializationException;

import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.Schema;

import com.linkedin.datastream.bigquery.schema.BigquerySchemaEvolver;
import com.linkedin.datastream.bigquery.translator.RecordTranslator;
import com.linkedin.datastream.bigquery.translator.SchemaTranslator;
import com.linkedin.datastream.common.DatastreamRecordMetadata;
import com.linkedin.datastream.common.Package;
import com.linkedin.datastream.metrics.DynamicMetricsManager;
import com.linkedin.datastream.serde.Deserializer;
import com.linkedin.datastream.server.api.transport.buffered.AbstractBatch;

/**
 * This class populates a batch of BQ rows to be committed.
 */
public class Batch extends AbstractBatch {

    private final int _maxBatchSize;
    private final int _maxBatchAge;
    private final BigqueryBatchCommitter _committer;

    private final List<InsertAllRequest.RowToInsert> _batch;
    private long _batchCreateTimeStamp;

    private org.apache.avro.Schema _avroSchema;
    private Schema _schema;
    private final BigqueryDatastreamDestination _destination;
    private final Deserializer _valueDeserializer;
    private final BigquerySchemaEvolver _schemaEvolver;
    private final org.apache.avro.Schema _fixedAvroSchema;

    /**
     * Constructor for Batch.
     * @param maxBatchSize any batch bigger than this threshold will be committed to BQ.
     * @param maxBatchAge any batch older than this threshold will be committed to BQ.
     * @param maxInflightWriteLogCommits maximum allowed batches in the commit backlog
     * @param valueDeserializer a Deserializer
     * @param committer committer object
     */
    public Batch(
            final BigqueryDatastreamDestination destination,
            final int maxBatchSize,
            final int maxBatchAge,
            final int maxInflightWriteLogCommits,
            final Deserializer valueDeserializer,
            final BigqueryBatchCommitter committer,
            final BigquerySchemaEvolver schemaEvolver,
            final org.apache.avro.Schema fixedAvroSchema
    ) {
        super(maxInflightWriteLogCommits);
        this._destination = destination;
        this._maxBatchSize = maxBatchSize;
        this._maxBatchAge = maxBatchAge;
        this._committer = committer;
        this._valueDeserializer = valueDeserializer;
        this._schemaEvolver = schemaEvolver;
        this._fixedAvroSchema = fixedAvroSchema;
        this._batch = new ArrayList<>();
        this._batchCreateTimeStamp = System.currentTimeMillis();
        this._schema = null;
    }

    private void reset() {
        _batch.clear();
        _ackCallbacks.clear();
        _recordMetadata.clear();
        _sourceTimestamps.clear();
        _batchCreateTimeStamp = System.currentTimeMillis();
    }

    @Override
    public void write(Package aPackage) throws InterruptedException {
        if (aPackage.isDataPackage()) {

            // skip null records
            if (aPackage.getRecord().getValue() == null) {
                LOG.info("Null record received from topic {}, partition {}, and offset {}",
                        aPackage.getTopic(), aPackage.getPartition(), aPackage.getOffset());
                DynamicMetricsManager.getInstance().createOrUpdateMeter(
                        this.getClass().getSimpleName(),
                        aPackage.getTopic(),
                        "nullRecordsCount",
                        1);
                aPackage.getAckCallback().onCompletion(new DatastreamRecordMetadata(
                        aPackage.getCheckpoint(), aPackage.getTopic(), aPackage.getPartition()), null);
                return;
            }

            final GenericRecord record;
            try {
                record = (GenericRecord) _valueDeserializer.deserialize((byte[]) aPackage.getRecord().getValue());
            } catch (SerializationException e) {
                if (e.getCause() instanceof BufferUnderflowException) {
                    LOG.warn("Error deserializing message at Topic {} - Partition {} - Offset {} - Reason {} - Exception {}",
                            aPackage.getTopic(),
                            aPackage.getPartition(),
                            aPackage.getOffset(),
                            e.getMessage(),
                            e.getCause().getClass());
                    DynamicMetricsManager.getInstance().createOrUpdateMeter(
                            this.getClass().getSimpleName(),
                            aPackage.getTopic(),
                            "deserializerErrorCount",
                            1);
                }
                throw e;
            }

            if (_fixedAvroSchema == null) {
                processAvroSchema(record.getSchema());
            } else {
                processAvroSchema(_fixedAvroSchema);
            }

            _batch.add(RecordTranslator.translate(record));

            _ackCallbacks.add(aPackage.getAckCallback());
            _recordMetadata.add(new DatastreamRecordMetadata(aPackage.getCheckpoint(),
                    aPackage.getTopic(),
                    aPackage.getPartition()));
            _sourceTimestamps.add(aPackage.getTimestamp());
        }

        if (aPackage.isForceFlushSignal() || _batch.size() >= _maxBatchSize ||
                System.currentTimeMillis() - _batchCreateTimeStamp >= _maxBatchAge) {
            flush(aPackage.isForceFlushSignal());
        }
    }

    private void processAvroSchema(final org.apache.avro.Schema avroSchema) {
        final Optional<Schema> newBQSchema;
        if (_avroSchema == null) {
            newBQSchema = Optional.of(SchemaTranslator.translate(avroSchema));
        } else if (!_avroSchema.equals(avroSchema)) {
            newBQSchema = Optional.of(_schemaEvolver.evolveSchema(_schema, SchemaTranslator.translate(avroSchema)));
        } else {
            newBQSchema = Optional.empty();
        }
        newBQSchema.ifPresent(schema -> {
            _avroSchema = avroSchema;
            _schema = schema;
            _committer.setDestTableSchema(_destination, _schema);
        });
    }

    private void flush(final boolean force) throws InterruptedException {
        if (_batch.isEmpty()) {
            LOG.debug("Nothing to flush.");
        } else {
            waitForRoomInCommitBacklog();
            incrementInflightWriteLogCommits();
            _committer.commit(
                    new ArrayList<>(_batch),
                    _destination.toString(),
                    new ArrayList<>(_ackCallbacks),
                    new ArrayList<>(_recordMetadata),
                    new ArrayList<>(_sourceTimestamps),
                    this::decrementInflightWriteLogCommitsAndNotify
            );
            reset();
            if (force) {
                waitForCommitBacklogToClear();
            }
        }
    }
}
