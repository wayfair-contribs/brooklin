/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.connectors.jdbc.cdc;

import com.linkedin.datastream.common.BrooklinEnvelope;
import com.linkedin.datastream.common.BrooklinEnvelopeMetadataConstants;
import com.linkedin.datastream.common.DatastreamRecordMetadata;
import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.metrics.DynamicMetricsManager;
import com.linkedin.datastream.server.DatastreamEventProducer;
import com.linkedin.datastream.server.DatastreamProducerRecord;
import com.linkedin.datastream.server.DatastreamProducerRecordBuilder;
import com.linkedin.datastream.server.FlushlessEventProducerHandler;
import com.linkedin.datastream.server.providers.CustomCheckpointProvider;
import com.linkedin.datastream.server.providers.KafkaCustomCheckpointProvider;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Connector task that reads data from SQL server and streams to the specified transportprovider.
 */
public class SQLServerCDCConnectorTask {

    private static final Logger LOG = LoggerFactory.getLogger(SQLServerCDCConnectorTask.class);

    private final ScheduledExecutorService _scheduler = Executors.newScheduledThreadPool(1);
    private final String _datastreamName;
    private final DataSource _dataSource;
    private final String _table;
    private final long _pollFrequencyMS;
    private final String _checkpointStorerURL;
    private final String _checkpointStoreTopic;
    private final String _destinationTopic;
    private final FlushlessEventProducerHandler<CDCCheckPoint> _flushlessProducer;
    private final int _maxPollRows;
    private final int _maxFetchSize;

    private String _id;
    private CustomCheckpointProvider<Long> _checkpointProvider;
    private AtomicBoolean _resetToSafeCommit;

    private static final String GET_MIN_LSN = "SELECT sys.fn_cdc_get_min_lsn('#')";
    private static final String GET_MAX_LSN = "SELECT sys.fn_cdc_get_max_lsn()";

    private static final String INCREMENT_LSN = "SELECT sys.fn_cdc_increment_lsn(?)";
    private static final String GET_ALL_CHANGES_FOR_TABLE = "SELECT * FROM cdc.[fn_cdc_get_all_changes_#](?, ?, N'all update old')";
    private static final String GET_ALL_CHANGES_AFTER = "SELECT * FROM cdc.[fn_cdc_get_all_changes_#](?, ?, N'all')";

    private String _cdc_query;
    private String _min_lsn_query;

    // todo: should be persistent
    private CDCCheckPoint _committedCheckpoint;
    private CDCCheckPoint _processingCheckpoint;

    private Schema _avroSchema;

    private SQLServerCDCConnectorTask(SQLServerCDCConnectorTaskBuilder builder) {
        _datastreamName = builder._datastreamName;
        _dataSource = builder._dataSource;
        _table = builder._table;
        _pollFrequencyMS = builder._pollFrequencyMS;
        _flushlessProducer = new FlushlessEventProducerHandler<>(builder._producer);
        _checkpointStorerURL = builder._checkpointStorerURL;
        _checkpointStoreTopic = builder._checkpointStoreTopic;
        _destinationTopic = builder._destinationTopic;
        _maxPollRows = builder._maxPollRows;
        _maxFetchSize = builder._maxFetchSize;

        _checkpointProvider = null;
        _resetToSafeCommit = new AtomicBoolean(false);

        generateQuery();
    }

    /**
     * start the task
     */
    public void start() {
        _id = generateID();
        _checkpointProvider = new KafkaCustomCheckpointProvider(_id, _checkpointStorerURL, _checkpointStoreTopic);
        _scheduler.scheduleWithFixedDelay(() -> {
                    try {
                        execute();
                    } catch (Exception e) {
                        LOG.error("Failed poll.", e);
                    }
                },
                _pollFrequencyMS,
                _pollFrequencyMS,
                TimeUnit.MILLISECONDS);
        LOG.info("SQLServer CDC Connector {} started", _id);
    }

    /**
     * stop the task
     */
    public void stop() {
        LOG.info("Stopping connector {}...", _id);

        _scheduler.shutdownNow();
        try {
            if (!_scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                LOG.warn("Task scheduler shutdown timed out.");
            }
            mayCommitCheckpoint();
            _checkpointProvider.close();
        } catch (InterruptedException e) {
            LOG.warn("Interrupted while awaiting task scheduler termination.");
            Thread.currentThread().interrupt();
        }
    }

    protected String generateID() {
        String idString = _datastreamName + "_" + _destinationTopic;
        long hash = idString.hashCode();
        return String.valueOf(hash > 0 ? hash : -hash);
    }

    protected synchronized void mayCommitCheckpoint() {
        Optional<CDCCheckPoint> safeCheckpoint = _flushlessProducer.getAckCheckpoint(_id, 0);
        if (safeCheckpoint.isPresent()) {
            LOG.debug("Last safe checkpoint is {}", safeCheckpoint.get());
            _committedCheckpoint = safeCheckpoint.get();
            if (_resetToSafeCommit.get()) {
                // todo: should call rewind ??
                _resetToSafeCommit.set(false);
            }
        }
    }

    protected void execute() {
        LOG.info("CDC poll initiated for task {}", _id);

        // Called right before a poll starts
        mayCommitCheckpoint();
        seekProcessingCheckpoint();

        try (Connection conn = _dataSource.getConnection();
            PreparedStatement preparedStatement = buildCDCStatement(conn);
            ResultSet rs = preparedStatement.executeQuery()) {

            processChanges(rs);
            LOG.info("CDC poll ends for task {}", _id);
        } catch (SQLException e) {
            LOG.error("Error in execute polling CDC table {}", _table, e);
            throw new DatastreamRuntimeException("Error in execute polling CDC table.", e);
        }
    }

    private void seekProcessingCheckpoint() {
        // read from committed checkpoint
        // if committed checkpoint is null. get initial value from db.
        if (_committedCheckpoint != null) {
            _processingCheckpoint = _committedCheckpoint;
            return;
        }

        try (Connection conn = _dataSource.getConnection()) {
            _committedCheckpoint = new CDCCheckPoint(_table, getMinLSN(conn), -1);
            _processingCheckpoint = _committedCheckpoint;
            LOG.info("CDC checkpoint: table = {}, checkpoint = {}", _table, _committedCheckpoint);
        } catch (SQLException e) {
            LOG.error("Failed to get min LSN for CDC table {}", _table, e);
            throw new DatastreamRuntimeException("Error in seeking processing checkpoint.", e);
        }
    }

    private byte[] getMinLSN(Connection conn) {
        try (PreparedStatement preparedStatement = conn.prepareStatement(_min_lsn_query)) {
            try (ResultSet rs = preparedStatement.executeQuery()) {
                return rs.next()
                        ? rs.getBytes(1)
                        : null;
            }
        } catch (SQLException e) {
            LOG.error("Failed to get FromLsn. The query is {}", _min_lsn_query, e);
        }

        return null;
    }

    private PreparedStatement buildCDCStatement(Connection conn) {
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = conn.prepareStatement(_cdc_query);

            preparedStatement.setBytes(1, _committedCheckpoint.lsnBytes());
            preparedStatement.setInt(2, _committedCheckpoint.offset()+1);
            preparedStatement.setInt(3, _maxPollRows);

            preparedStatement.setFetchSize(_maxFetchSize);
        } catch (SQLException e) {
            LOG.error("Error in build CDC statement", e);
            throw new DatastreamRuntimeException("Error in build CDC statement", e);
        }

        return preparedStatement;
    }

    private void generateQuery() {
        // todo: support more modes

        // _cdc_query = GET_ALL_CHANGES_AFTER.replace(PLACE_HOLDER, _table);
        // _cdc_query = GET_ALL_CHANGES_FOR_TABLE.replace(PLACE_HOLDER, _table);

        _min_lsn_query = GET_MIN_LSN.replace(CDCQueryBuilder.PLACE_HOLDER, _table);
        _cdc_query = new CDCQueryBuilderWithOffset(_table, true).generate();
    }

    private boolean getNext(ResultSet resultSet) throws SQLException{
        boolean hasNext;
        long startTime = System.currentTimeMillis();
        hasNext = resultSet.next();

        DynamicMetricsManager.getInstance().createOrUpdateHistogram(
                this.getClass().getSimpleName(), "getNext", "exec_time",
                System.currentTimeMillis() - startTime);
        return hasNext;

    }

    private void processChanges(ResultSet resultSet) {

        int resultSetSize = 0;

        Schema avroSchema = getSchemaFromResultSet(resultSet);

        ChangeEvent preEvent = null;
        try {
            while (getNext(resultSet)) {

                long startTime = System.currentTimeMillis();
                resultSetSize++;
                updateProcessingCheckpoint(resultSet);

                // for update before row, buffer and continue loop
                int op = resultSet.getInt(CDCQueryBuilder.OP_INDEX);
                ChangeEvent curEvent = null;
                if (op == 3) { // the row before update
                    // create a partial event
                    curEvent = buildEventWithUpdateBefore(resultSet, _processingCheckpoint);
                } else if (op == 4) { // the row after update
                    // complete event
                    curEvent = completeEventWithUpdateAfter(preEvent, resultSet, _processingCheckpoint);
                } else { // insert or delete
                    curEvent = buildEventWithInsertOrDelete(op, resultSet, _processingCheckpoint);
                }

                if (!curEvent.isComplete()) {
                    preEvent = curEvent;
                    continue;
                } else {
                    preEvent = null;
                }

                if (LOG.isTraceEnabled()) {
                    LOG.trace("Translating and sending the event {}", _processingCheckpoint);
                }

                GenericRecord record = AvroRecordTranslator.fromChangeEvent(avroSchema, curEvent);

                HashMap<String, String> meta = new HashMap<>();
                meta.put(BrooklinEnvelopeMetadataConstants.EVENT_TIMESTAMP, String.valueOf(System.currentTimeMillis()));

                BrooklinEnvelope envelope = new BrooklinEnvelope(_processingCheckpoint.toString().getBytes(), record, null, meta);

                DatastreamProducerRecordBuilder builder = new DatastreamProducerRecordBuilder();
                builder.addEvent(envelope);
                builder.setEventsSourceTimestamp(System.currentTimeMillis());
                DatastreamProducerRecord producerRecord = builder.build();

                DynamicMetricsManager.getInstance().createOrUpdateHistogram(
                        this.getClass().getSimpleName(), "translate", "exec_time",
                        System.currentTimeMillis() - startTime);

                _flushlessProducer.send(producerRecord, _id, 0, _processingCheckpoint,
                        (DatastreamRecordMetadata metadata, Exception exception) -> {
                            if (exception == null) {
                                if (LOG.isTraceEnabled()) {
                                    LOG.trace("sent record {} successfully", metadata.getCheckpoint());
                                }
                            } else {
                                _resetToSafeCommit.set(true);
                                LOG.warn("failed to send record {}.", metadata.getCheckpoint(), exception);
                            }
                        });

                // todo
                // _checkpointProvider.updateCheckpoint(checkpoint);
            }
        } catch (SQLException e) {
            LOG.error("Error in process change events.", e);
            throw new DatastreamRuntimeException(e);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("handled {} cdc events. processing checkpoint is {}", resultSetSize, _processingCheckpoint);
        }
    }

    private void updateProcessingCheckpoint(ResultSet row) {
        try {
            byte[] lsn = row.getBytes(CDCQueryBuilder.LSN_INDEX);
            int offset = Arrays.equals(lsn, _processingCheckpoint.lsnBytes())
                    ? _processingCheckpoint.offset()+1 : 0;
            _processingCheckpoint = new CDCCheckPoint(_table, lsn, offset);
        } catch (SQLException e) {
            throw new DatastreamRuntimeException(e);
        }
    }

    private Schema getSchemaFromResultSet(ResultSet rs) {
        try {
            ResultSetMetaData metaData = rs.getMetaData();
            // Use cached schema or parse from ResultSet
            if (_avroSchema == null) {
                _avroSchema = SchemaTranslator.fromResultSet(metaData);
            }
            return _avroSchema;
        } catch (SQLException e) {
            LOG.error("Failed to retrieve the schema of table {}", _table);
            throw new DatastreamRuntimeException(e);
        }
    }

    private ChangeEvent buildEventWithInsertOrDelete(int op, ResultSet row, CDCCheckPoint checkpoint) {
        long txTs = readTransactionTime(row);
        ChangeEvent event = new ChangeEvent(null, null, op, txTs);
        return completeEventWithUpdateAfter(event, row, checkpoint);
    }

    private ChangeEvent buildEventWithUpdateBefore(ResultSet row, CDCCheckPoint checkpoint) {
        long txTs = readTransactionTime(row);
        byte[] seqVal = readSeqVal(row);
        Object[] dataBefore = readDataColumns(row);

        ChangeEvent event = new ChangeEvent(checkpoint, seqVal, 3, txTs);
        event.setUpdateBefore(dataBefore);
        return event;
    }

    private ChangeEvent completeEventWithUpdateAfter(ChangeEvent preEvent, ResultSet row, CDCCheckPoint checkpoint) {
        byte[] seqVal = readSeqVal(row);
        Object[] dataAfter = readDataColumns(row);
        preEvent.completeUpdate(dataAfter, seqVal, checkpoint);
        return preEvent;
    }

    private long readTransactionTime(ResultSet row) {
        try {
            Timestamp ts = row.getTimestamp(CDCQueryBuilder.TRANSACTION_TIME_INDEX);
            return ts == null ? 0 : ts.getTime();
        } catch (SQLException e) {
            throw new DatastreamRuntimeException(e);
        }
    }

    private byte[] readSeqVal(ResultSet row) {
        try {
            return row.getBytes(CDCQueryBuilder.SEQVAL_INDEX);
        } catch (SQLException e) {
            throw new DatastreamRuntimeException(e);
        }
    }

    private Object[] readDataColumns(ResultSet row) {
        try {
            int numCols = row.getMetaData().getColumnCount();
            int size = numCols - CDCQueryBuilder.META_COLUMN_NUM;
            Object[] data = new Object[size];  // exclude 5 meta columns
            //  read columns from 6 to last-1.
            for (int i = CDCQueryBuilder.DATA_COLUMN_START_INDEX; i < numCols; i++) {
                data[i - CDCQueryBuilder.DATA_COLUMN_START_INDEX] = row.getObject(i);
            }
            return data;
        } catch (SQLException e) {
            throw new DatastreamRuntimeException("Failed to read data columns in each row", e);
        }
    }

    /**
     * builder class for SQLServerCDCConnectorTask
     */
    public static class SQLServerCDCConnectorTaskBuilder {
        private String _datastreamName;
        private DatastreamEventProducer _producer;

        private DataSource _dataSource;
        private String _table = null;
        private long _pollFrequencyMS = 60000;
        private int _maxPollRows;
        private int _maxFetchSize;

        private String _checkpointStorerURL;
        private String _checkpointStoreTopic;
        private String _destinationTopic;

        /**
         * set the datastream name
         *
         * @param datastreamName datastream name
         * @return instance of SQLServerCDCConnectorTaskBuilder
         */
        public SQLServerCDCConnectorTask.SQLServerCDCConnectorTaskBuilder setDatastreamName(String datastreamName) {
            this._datastreamName = datastreamName;
            return this;
        }

        /**
         * set the sql data source
         *
         * @param dataSource datasource
         * @return instance of SQLServerCDCConnectorTaskBuilder
         */
        public SQLServerCDCConnectorTask.SQLServerCDCConnectorTaskBuilder setDataSource(DataSource dataSource) {
            this._dataSource = dataSource;
            return this;
        }

        /**
         * set the table to query
         *
         * @param table table name
         * @return instance of SQLServerCDCConnectorTaskBuilder
         */
        public SQLServerCDCConnectorTask.SQLServerCDCConnectorTaskBuilder setTable(String table) {
            this._table = table;
            return this;
        }

        /**
         * set the poll frequency
         *
         * @param pollFrequencyMS poll frequency in milli seconds
         * @return instance of SQLServerCDCConnectorTaskBuilder
         */
        public SQLServerCDCConnectorTask.SQLServerCDCConnectorTaskBuilder setPollFrequencyMS(long pollFrequencyMS) {
            this._pollFrequencyMS = pollFrequencyMS;
            return this;
        }

        /**
         * set the event producer
         *
         * @param producer event producer
         * @return instance of SQLServerCDCConnectorTaskBuilder
         */
        public SQLServerCDCConnectorTask.SQLServerCDCConnectorTaskBuilder setEventProducer(DatastreamEventProducer producer) {
            this._producer = producer;
            return this;
        }

        /**
         * set the checkpoint store url
         *
         * @param checkpointStoreURL checkpoint store url
         * @return instance of SQLServerCDCConnectorTaskBuilder
         */
        public SQLServerCDCConnectorTask.SQLServerCDCConnectorTaskBuilder setCheckpointStoreURL(String checkpointStoreURL) {
            this._checkpointStorerURL = checkpointStoreURL;
            return this;
        }

        /**
         * set the checkpoint store topic
         *
         * @param checkpointStoreTopic topic name
         * @return instance of SQLServerCDCConnectorTaskBuilder
         */
        public SQLServerCDCConnectorTask.SQLServerCDCConnectorTaskBuilder setCheckpointStoreTopic(String checkpointStoreTopic) {
            this._checkpointStoreTopic = checkpointStoreTopic;
            return this;
        }

        /**
         * set the destination topic
         *
         * @param destinationTopic topic to produce to
         * @return instance of SQLServerCDCConnectorTaskBuilder
         */
        public SQLServerCDCConnectorTask.SQLServerCDCConnectorTaskBuilder setDestinationTopic(String destinationTopic) {
            this._destinationTopic = destinationTopic;
            return this;
        }

        /**
         * set max rows to poll
         *
         * @param maxPollRows max number of rows to poll
         * @return instance of SQLServerCDCConnectorTaskBuilder
         */
        public SQLServerCDCConnectorTask.SQLServerCDCConnectorTaskBuilder setMaxPollRows(int maxPollRows) {
            this._maxPollRows = maxPollRows;
            return this;
        }

        /**
         * set max fetch size
         *
         * @param maxFetchSize max rows to fetch in each network call
         * @return instance of SQLServerCDCConnectorTaskBuilder
         */
        public SQLServerCDCConnectorTask.SQLServerCDCConnectorTaskBuilder setMaxFetchSize(int maxFetchSize) {
            this._maxFetchSize = maxFetchSize;
            return this;
        }

        /**
         * build connector task
         *
         * @return instance of SQLServerCDCConnectorTask
         */
        public SQLServerCDCConnectorTask build() {
            return new SQLServerCDCConnectorTask(this);
        }
    }
}
