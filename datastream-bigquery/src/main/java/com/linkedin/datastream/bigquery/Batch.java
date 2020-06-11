package com.linkedin.datastream.bigquery;

import com.google.api.services.bigquery.model.TableDataInsertAllRequest;
import com.linkedin.datastream.common.DatastreamRecordMetadata;
import com.linkedin.datastream.common.Package;
import com.linkedin.datastream.common.VerifiableProperties;
import com.linkedin.datastream.server.api.transport.buffered.AbstractBatch;
import com.linkedin.datastream.server.api.transport.buffered.BatchCommitter;

import java.util.ArrayList;
import java.util.List;

public class Batch extends AbstractBatch {

    private final int _maxBatchSize;
    private final int _maxBatchAge;
    private final VerifiableProperties _conversionProperties;
    private final BatchCommitter<TableDataInsertAllRequest.Rows> _committer;

    private final List<TableDataInsertAllRequest.Rows> _batch;
    private long _batchCreateTimeStamp;

    public Batch(int maxBatchSize,
                 int maxBatchAge,
                 int maxInflightWriteLogCommits,
                 VerifiableProperties conversionProperties,
                 BatchCommitter<TableDataInsertAllRequest.Rows> committer) {
        super(maxInflightWriteLogCommits);
        this._maxBatchSize = maxBatchSize;
        this._maxBatchAge = maxBatchAge;
        this._conversionProperties = conversionProperties;
        this._committer = committer;
        this._batch = new ArrayList<>();
        _batchCreateTimeStamp = System.currentTimeMillis();
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
            _batch.add();
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
                    _batch,
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
