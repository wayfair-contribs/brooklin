package com.linkedin.datastream.bigquery;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.Schema;
import com.linkedin.datastream.bigquery.translator.RecordTranslator;
import com.linkedin.datastream.bigquery.translator.SchemaTranslator;
import com.linkedin.datastream.common.DatastreamRecordMetadata;
import com.linkedin.datastream.common.Package;
import com.linkedin.datastream.common.SchemaRegistry;
import com.linkedin.datastream.server.api.transport.buffered.AbstractBatch;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;

import org.apache.avro.generic.GenericRecord;

public class Batch extends AbstractBatch {

    private final int _maxBatchSize;
    private final int _maxBatchAge;
    private final BigqueryBatchCommitter _committer;

    private final List<InsertAllRequest.RowToInsert> _batch;
    private long _batchCreateTimeStamp;

    private org.apache.avro.Schema _avroSchema;
    private Schema _schema;
    private SchemaRegistry _schemaRegistry;
    private String _destination;
    private KafkaAvroDeserializer _deserializer;

    public Batch(int maxBatchSize,
                 int maxBatchAge,
                 int maxInflightWriteLogCommits,
                 SchemaRegistry schemaRegistry,
                 BigqueryBatchCommitter committer) {
        super(maxInflightWriteLogCommits);
        this._maxBatchSize = maxBatchSize;
        this._maxBatchAge = maxBatchAge;
        this._committer = committer;
        this._batch = new ArrayList<>();
        this._batchCreateTimeStamp = System.currentTimeMillis();
        this._schema = null;
        this._destination = null;
        this._schemaRegistry = schemaRegistry;
        this._deserializer = _schemaRegistry.getDeserializer();
    }

    private void reset() {
        _batch.clear();
        _destination = null;
        _ackCallbacks.clear();
        _recordMetadata.clear();
        _batchCreateTimeStamp = System.currentTimeMillis();
    }

    public void write(Package aPackage) throws InterruptedException {
        if (aPackage.isDataPackage()) {

            if (_destination == null) {
                String[] datasetTableSuffix = aPackage.getDestination().split("/");
                if (datasetTableSuffix.length == 2) {
                    _destination = datasetTableSuffix[0] + "/" + aPackage.getTopic() + datasetTableSuffix[1];
                } else {
                    _destination = datasetTableSuffix[0] + "/" + aPackage.getTopic();
                }
            }

            if (_schema == null) {
                _avroSchema = _schemaRegistry.getSchemaByTopic(aPackage.getTopic());
                _schema = SchemaTranslator.translate(_avroSchema);
                _committer.setDestTableSchema(_destination, _schema);
            }

            _batch.add(RecordTranslator.translate(
                            (GenericRecord) _deserializer.deserialize(
                                    aPackage.getTopic(),
                                    (byte[]) aPackage.getRecord().getValue()),
                            _avroSchema));

            _ackCallbacks.add(aPackage.getAckCallback());
            _recordMetadata.add(new DatastreamRecordMetadata(aPackage.getCheckpoint(),
                    aPackage.getTopic(),
                    aPackage.getPartition()));
        } else if (aPackage.isTryFlushSignal() || aPackage.isForceFlushSignal()) {
            if (_batch.isEmpty()) {
                LOG.debug("Nothing to flush.");
                return;
            }
        }
        if (_batch.size() >= _maxBatchSize ||
                System.currentTimeMillis() - _batchCreateTimeStamp >= _maxBatchAge ||
                aPackage.isForceFlushSignal()) {
            waitForRoomInCommitBacklog();
            incrementInflightWriteLogCommits();
            _committer.commit(
                    new ArrayList<>(_batch),
                    _destination,
                    new ArrayList<>(_ackCallbacks),
                    new ArrayList<>(_recordMetadata),
                    () -> decrementInflightWriteLogCommitsAndNotify()
            );
            reset();

            if (aPackage.isForceFlushSignal()) {
                waitForCommitBacklogToClear();
            }
        }
    }
}
