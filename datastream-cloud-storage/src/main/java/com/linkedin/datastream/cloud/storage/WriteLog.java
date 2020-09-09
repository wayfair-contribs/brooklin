/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.cloud.storage;

import java.io.IOException;

import java.util.ArrayList;
import java.util.List;

import com.linkedin.datastream.common.DatastreamTransientException;
import com.linkedin.datastream.common.DatastreamRecordMetadata;
import com.linkedin.datastream.common.Package;
import com.linkedin.datastream.common.ReflectionUtils;
import com.linkedin.datastream.common.SendCallback;
import com.linkedin.datastream.common.VerifiableProperties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;

import com.linkedin.datastream.cloud.storage.committer.ObjectCommitter;
import com.linkedin.datastream.cloud.storage.io.File;

import com.linkedin.datastream.metrics.DynamicMetricsManager;

/**
 * This class writes the messages to local file. Which can be committed to cloud storage later.
 */
public class WriteLog {
    private static final Logger LOG = LoggerFactory.getLogger(WriteLog.class.getName());

    private final String _ioClass;
    private final VerifiableProperties _ioProperties;
    private final String _localDir;
    private final long _maxFileSize;
    private final int _maxFileAge;
    private final ObjectCommitter _committer;
    private final int _maxInflightWriteLogCommits;

    private final Meter _writeRateMeter;

    private Exception exception = null;

    private File _file;
    private String _destination;
    private String _topic;
    private long _partition;
    private long _minOffset;
    private long _maxOffset;
    private List<SendCallback> _ackCallbacks;
    private List<DatastreamRecordMetadata> _recordMetadata;

    private int _inflightWriteLogCommits;
    private Object _counterLock;
    private Configuration _conf = null;
    private long _prevFailedOffset;
    private long _lastSeenOffset;


    private long _fileStartTime;

    /**
     * Constructor for WriteLog.
     * @param ioClass one of the implementations of {@link com.linkedin.datastream.cloud.storage.io.File}
     * @param ioProperties configuration options for the io class
     * @param localDir local directory where the objects are created before committing to cloud storage
     * @param maxFileSize max size of the object
     * @param maxFileAge max age of the object
     * @param maxInflightWriteLogCommits max commit backlog
     * @param committer object committer
     */
    public WriteLog(String ioClass,
                    VerifiableProperties ioProperties,
                    String localDir,
                    long maxFileSize,
                    int maxFileAge,
                    int maxInflightWriteLogCommits,
                    ObjectCommitter committer) {
        this._ioClass = ioClass;
        this._ioProperties = ioProperties;
        this._maxInflightWriteLogCommits = maxInflightWriteLogCommits;
        this._inflightWriteLogCommits = 0;
        this._maxFileSize = maxFileSize;
        this._maxFileAge = maxFileAge;
        this._file = null;
        this._fileStartTime = System.currentTimeMillis();
        this._localDir = localDir;
        this._committer = committer;
        this._ackCallbacks = new ArrayList<>();
        this._recordMetadata = new ArrayList<>();
        this._destination = null;
        this._topic = null;
        this._partition = 0;
        this._minOffset = Long.MAX_VALUE;
        this._maxOffset = Long.MIN_VALUE;
        this._prevFailedOffset = Long.MAX_VALUE;
        this._conf = new Configuration();
        this._counterLock = new Object();
        this._writeRateMeter = DynamicMetricsManager.getInstance().registerMetric(this.getClass().getSimpleName(),
                "writeRate", Meter.class);
    }

    private String getFilePath(String topic, String partition) {
        return new StringBuilder()
                .append(_localDir)
                .append("/")
                .append(topic)
                .append("+")
                .append(partition)
                .append("+")
                .append(System.currentTimeMillis()).toString();
    }

    private void waitForRoomInCommitBacklog() throws InterruptedException {
        synchronized (_counterLock) {
            if (_inflightWriteLogCommits >= _maxInflightWriteLogCommits) {
                LOG.info("Waiting for room in commit backlog, current inflight commits {} ",
                        _inflightWriteLogCommits);
            }
            while (_inflightWriteLogCommits >= _maxInflightWriteLogCommits) {
                try {
                    _counterLock.wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw e;
                }
            }
        }
    }

    private void waitForCommitBacklogToClear() throws InterruptedException {
        synchronized (_counterLock) {
            if (_inflightWriteLogCommits > 0) {
                LOG.info("Waiting for the commit backlog to clear.");
            }
            while (_inflightWriteLogCommits > 0) {
                try {
                    _counterLock.wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw e;
                }
            }
        }
    }

    private void incrementInflightWriteLogCommits() {
        synchronized (_counterLock) {
            _inflightWriteLogCommits++;
        }
    }

    private void decrementInflightWriteLogCommitsAndNotify() {
        synchronized (_counterLock) {
            _inflightWriteLogCommits--;
            _counterLock.notify();
        }
    }

    private void reset() {
        _file = null;
        _destination = null;
        _topic = null;
        _partition = 0;
        _minOffset = Long.MAX_VALUE;
        _maxOffset = Long.MIN_VALUE;
        _ackCallbacks.clear();
        _recordMetadata.clear();
    }

    private static void deleteFile(java.io.File file) {
        LOG.info("Deleting file {}", file.toPath());
        if (!file.delete()) {
            LOG.warn("Failed to delete file {}.", file.toPath());
        }

        // clean crc files
        final java.io.File crcFile = new java.io.File(file.getParent() + "/." + file.getName() + ".crc");
        if (crcFile.exists() && crcFile.isFile()) {
            if (!crcFile.delete()) {
                LOG.warn("Failed to delete crc file {}.", crcFile.toPath());
            }
        }
    }

    /**
     * writes the given record in the package to the write log
     * @param aPackage package to be written
     * @throws IOException
     */
    public void write(Package aPackage) throws IOException, InterruptedException {
        if (aPackage.isDataPackage()) {

            if (_prevFailedOffset != Long.MAX_VALUE) {
                if (aPackage.getOffset() < _prevFailedOffset) {
                    aPackage.getAckCallback().onCompletion(new DatastreamRecordMetadata(aPackage.getCheckpoint(),
                                    aPackage.getTopic(),
                                    aPackage.getPartition())
                            , null);
                    LOG.info("Packet with already processed offset arrived");
                    return;
                } else if (aPackage.getOffset() == _lastSeenOffset + 1) {
                    aPackage.getAckCallback().onCompletion(new DatastreamRecordMetadata(aPackage.getCheckpoint(),
                                    aPackage.getTopic(),
                                    aPackage.getPartition())
                            , new DatastreamTransientException("High offset record")); // TODO: add message
                    _lastSeenOffset = aPackage.getOffset();
                    LOG.info("Packet with future offset can't be processed");
                    return;
                } else if (aPackage.getOffset() == _prevFailedOffset + 1) {
                    _prevFailedOffset = Long.MAX_VALUE;
                    LOG.info("Packet with desired offset found");
                } else if (aPackage.getOffset() > _lastSeenOffset) {
                    _prevFailedOffset = Long.MAX_VALUE;
                    LOG.info("Packet drop");
                }
            }

            if (_prevFailedOffset == Long.MAX_VALUE) {
                LOG.info("Start processing and persisting records in local file");
                final String filePath = getFilePath(aPackage.getTopic(), String.valueOf(aPackage.getPartition()));
                if (_file == null) {
                    _file = ReflectionUtils.createInstance(_ioClass, filePath, _ioProperties);
                    _destination = aPackage.getDestination();
                    _topic = aPackage.getTopic();
                    _partition = aPackage.getPartition();
                    _fileStartTime = System.currentTimeMillis();
                }
                _writeRateMeter.mark();
                _file.write(aPackage);
                _maxOffset = (aPackage.getOffset() > _maxOffset) ? aPackage.getOffset() : _maxOffset;
                _minOffset = (aPackage.getOffset() < _minOffset) ? aPackage.getOffset() : _minOffset;
                _ackCallbacks.add(aPackage.getAckCallback());
                _recordMetadata.add(new DatastreamRecordMetadata(aPackage.getCheckpoint(),
                        aPackage.getTopic(),
                        aPackage.getPartition()));
            }
            _lastSeenOffset = aPackage.getOffset();
        } else if (aPackage.isTryFlushSignal() || aPackage.isForceFlushSignal()) {
            if (_file == null) {
                LOG.debug("Nothing to flush.");
                return;
            }
        }

        if (_file.length() >= _maxFileSize ||
                System.currentTimeMillis() - _fileStartTime >= _maxFileAge ||
                aPackage.isForceFlushSignal()) {
            waitForRoomInCommitBacklog();
            incrementInflightWriteLogCommits();
            _file.close();
            LOG.info("File ready to be consumed by committer");
            try {
                ParquetMetadata parquetMetadata = ParquetFileReader.readFooter(_conf, (Path) _file, ParquetMetadataConverter.SKIP_ROW_GROUPS);
                if (parquetMetadata.getBlocks().isEmpty()) {
                    LOG.info("Corrupt file identified");
                    _prevFailedOffset = _minOffset;
                    exception = new DatastreamTransientException("Drop corrupt record"); // TODO: add message
                    for (int i = 0; i < _ackCallbacks.size(); i++) {
                        _ackCallbacks.get(i).onCompletion(_recordMetadata.get(i), exception);
                    }
                    DynamicMetricsManager.getInstance().createOrUpdateMeter(
                            this.getClass().getSimpleName(),
                            _recordMetadata.get(0).getTopic(),
                            "errorCount",
                            1);
                    DynamicMetricsManager.getInstance().createOrUpdateMeter(
                            this.getClass().getSimpleName(),
                            _recordMetadata.get(0).getTopic(),
                            "commitCount",
                            1);
                    reset();
                    deleteFile(new java.io.File(_file.getPath()));
                } else {
                    _committer.commit(
                            _file.getPath(),
                            _file.getFileFormat(),
                            _destination,
                            _topic,
                            _partition,
                            _minOffset,
                            _maxOffset,
                            new ArrayList<>(_ackCallbacks),
                            new ArrayList<>(_recordMetadata),
                            () -> decrementInflightWriteLogCommitsAndNotify()
                    );
                    LOG.info("File committed");
                    reset();
                    if (aPackage.isForceFlushSignal()) {
                        waitForCommitBacklogToClear();
                    }

                }
            } catch (RuntimeException e) {
                LOG.error("Parquet file check for corruption Failed" + e);
            } catch (IOException e) {
                LOG.error("Parquet file check for corruption Failed" + e);
            }
        }
    }
}
