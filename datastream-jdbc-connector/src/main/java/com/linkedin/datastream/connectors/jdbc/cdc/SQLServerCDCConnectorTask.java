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

    private String _id;
    private final String _datastreamName;

    private final String _table;

    private final String _checkpointStorerURL;
    private final String _checkpointStoreTopic;
    private final String _destinationTopic;

    private final long _pollFrequencyMS;
    private final int _maxPollRows;
    private final int _maxFetchSize;

    private final ScheduledExecutorService _scheduler = Executors.newScheduledThreadPool(1);
    private final DataSource _dataSource;
    private final FlushlessEventProducerHandler<CDCCheckPoint> _flushlessProducer;
    private CDCRowReader _rowReader;

    private static final String GET_MIN_LSN = "SELECT sys.fn_cdc_get_min_lsn('#')";

    private String _cdc_query;
    private String _min_lsn_query;

    private CDCCheckPoint _committingCheckpoint;
    private CustomCheckpointProvider<CDCCheckPoint, CDCCheckPoint.Deserializer> _checkpointProvider;
    private AtomicBoolean _resetToSafeCommit;

    // Cache of avro schema
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

        _rowReader = new CDCRowReader();

        generateQuery();
    }

    /**
     * start the task
     */
    public void start() {
        _id = generateID();
        _checkpointProvider = new KafkaCustomCheckpointProvider<>(_id, _checkpointStorerURL, _checkpointStoreTopic);
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
            processCheckpoint();
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

    protected synchronized void processCheckpoint() {
        Optional<CDCCheckPoint> safeCheckpoint = _flushlessProducer.getAckCheckpoint(_id, 0);

        // Commit checkpoint to CheckpointProvider if there's new checkpoint
        if (safeCheckpoint.isPresent()) {
            _checkpointProvider.commit(safeCheckpoint.get());

            if (_resetToSafeCommit.get()) { // Failure happened, rewind committingCheckpoint
                _committingCheckpoint = safeCheckpoint.get();
                _resetToSafeCommit.set(false);
                LOG.info("rewinded the committing point to {} ", _committingCheckpoint);
            }
            LOG.info("Got committing checkpoint from acknowledged checkpoint: {}", _committingCheckpoint);
        }

        if (_committingCheckpoint == null) {
            // try getting checkpoint from CheckpointProvider.
            _committingCheckpoint = _checkpointProvider.getSafeCheckpoint(
                    () -> new CDCCheckPoint.Deserializer(),
                    CDCCheckPoint.class);
            LOG.info("Got committing checkpoint from persistent provider: {}", _committingCheckpoint);
        }

        if (_committingCheckpoint == null) {
            // try getting initial checkpoint from DB
            try (Connection conn = _dataSource.getConnection()) {
                _committingCheckpoint = new CDCCheckPoint(getMinLSN(conn), -1);
                LOG.info("Got committing checkpoint from DB: {}", _committingCheckpoint);
            } catch (SQLException e) {
                LOG.error("Failed to get min LSN for CDC table {}", _table, e);
                throw new DatastreamRuntimeException(e);
            }
        }
    }

    protected void execute() {
        LOG.info("CDC poll initiated for task = {}, capture instance = {}", _id, _table);

        // Called right before a poll starts
        processCheckpoint();

        try (Connection conn = _dataSource.getConnection();
            PreparedStatement preparedStatement =
                    buildCDCStatement(conn, _committingCheckpoint.lsnBytes(), _committingCheckpoint.offset()+1);
            ResultSet rs = preparedStatement.executeQuery()) {
            processChanges(rs);
            LOG.info("CDC poll ends for task {}", _id);
        } catch (SQLException e) {
            LOG.error("Error in execute polling CDC table {}", _table, e);
            throw new DatastreamRuntimeException("Error in execute polling CDC table.", e);
        }
    }

    private byte[] getMinLSN(Connection conn) {
        try (PreparedStatement preparedStatement = conn.prepareStatement(_min_lsn_query);
                ResultSet rs = preparedStatement.executeQuery() ) {

            return rs.next()
                        ? rs.getBytes(1)
                        : null;
        } catch (SQLException e) {
            LOG.error("Failed to get FromLsn. The query is {}", _min_lsn_query, e);
        }

        return null;
    }

    private PreparedStatement buildCDCStatement(Connection conn, byte[] fromLsn, int offset) {
        LOG.info("Generating CDC query statement: LSN = {}, offset = {}", Utils.bytesToHex(fromLsn), offset);
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = conn.prepareStatement(_cdc_query);

            preparedStatement.setBytes(1, fromLsn);
            preparedStatement.setInt(2, offset);
            preparedStatement.setInt(3, _maxPollRows);

            preparedStatement.setFetchSize(_maxFetchSize);

            LOG.info("===== Actual sql is {}", preparedStatement.toString());
        } catch (SQLException e) {
            LOG.error("Error in build CDC statement", e);
            throw new DatastreamRuntimeException("Error in build CDC statement", e);
        }

        return preparedStatement;
    }

    private void generateQuery() {
        // todo: support more modes
        _min_lsn_query = GET_MIN_LSN.replace(CDCQueryBuilder.PLACE_HOLDER, _table);
        _cdc_query = new CDCQueryBuilderWithOffset(_table, true).generate();
    }

    private void processChanges(ResultSet resultSet) {
        int resultSetSize = 0;

        Schema avroSchema = getSchemaFromResultSet(resultSet);

        // preEvent will have value when updateBefore has been received while updateAfter has not.
        ChangeEvent preEvent = null;
        while (_rowReader.next(resultSet)) {
            long processStartTime = System.currentTimeMillis();
            resultSetSize++;

            // create checkpoint from current row and update committingCheckpoint after the row is
            // sent to TransportProvider
            CDCCheckPoint checkpoint = createCheckpoint(resultSet);

            // Create an event object for each change event
            // Each insert or delete has 1 event object.
            // Each update needs to merge 2 rows into one event object.
            int op = _rowReader.readOperation();
            LOG.info("got event op = {}, seq = {}, cmdid = {}", op, Utils.bytesToHex(_rowReader.readSeqVal()), _rowReader.readCommandId() );
            ChangeEvent curEvent = null;
            if (op == 3) { // the row before update
                // create a partial event
                curEvent = buildEventWithUpdateBefore(resultSet, checkpoint);
            } else if (op == 4) { // the row after update
                // complete event
                curEvent = completeEventWithUpdateAfter(preEvent, resultSet, checkpoint);
            } else { // insert or delete
                curEvent = buildEventWithInsertOrDelete(op, resultSet, checkpoint);
            }

            if (!curEvent.isComplete()) { // This is update before, wait for next row.
                preEvent = curEvent;
                continue;
            } else {
                preEvent = null;
            }

            // Transform to Avro
            GenericRecord record = AvroRecordTranslator.fromChangeEvent(avroSchema, curEvent);

            // Send to KafkaTransportProvider
            HashMap<String, String> meta = new HashMap<>();
            meta.put(BrooklinEnvelopeMetadataConstants.EVENT_TIMESTAMP, String.valueOf(System.currentTimeMillis()));

            BrooklinEnvelope envelope = new BrooklinEnvelope(checkpoint.toString().getBytes(), record, null, meta);

            DatastreamProducerRecordBuilder builder = new DatastreamProducerRecordBuilder();
            builder.addEvent(envelope);
            builder.setEventsSourceTimestamp(processStartTime);
            builder.setEventsProduceTimestamp(curEvent.getTxStartTime());
            DatastreamProducerRecord producerRecord = builder.build();

            _flushlessProducer.send(producerRecord, _id, 0, checkpoint,
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

            // Here the row has been sent to TP, update committingCheckpoint
            _committingCheckpoint = checkpoint;
        }

        // Last row in this poll is updateBefore. Need to rewind committing checkpoint.
        if (preEvent != null) {
            LOG.info("Last event is not completed. Rewind checkpoint");
            _resetToSafeCommit.set(true);
        }

        LOG.info("handled {} cdc events for captureInstance = {}. committing checkpoint is {}", resultSetSize, _table, _committingCheckpoint);
    }

    private CDCCheckPoint createCheckpoint(ResultSet row) {
        try {
            byte[] lsn = row.getBytes(CDCQueryBuilder.LSN_INDEX);
            int offset = Arrays.equals(lsn, _committingCheckpoint.lsnBytes())
                    ? _committingCheckpoint.offset()+1 : 0;
            return new CDCCheckPoint(lsn, offset);
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
        long txTs = _rowReader.readTransactionTime();
        ChangeEvent event = new ChangeEvent(null, null, op, txTs);
        return completeEventWithUpdateAfter(event, row, checkpoint);
    }

    private ChangeEvent buildEventWithUpdateBefore(ResultSet row, CDCCheckPoint checkpoint) {
        LOG.info("buildEventWithUpdateBefore");
        long txTs = _rowReader.readTransactionTime();
        byte[] seqVal = _rowReader.readSeqVal();
        Object[] dataBefore = _rowReader.readDataColumns();

        ChangeEvent event = new ChangeEvent(checkpoint, seqVal, 3, txTs);
        event.setUpdateBefore(dataBefore);
        return event;
    }

    private ChangeEvent completeEventWithUpdateAfter(ChangeEvent preEvent, ResultSet row, CDCCheckPoint checkpoint) {
        LOG.info("completeEventWithUpdateAfter");
        byte[] seqVal = _rowReader.readSeqVal();
        Object[] dataAfter = _rowReader.readDataColumns();
        preEvent.completeUpdate(dataAfter, seqVal, checkpoint);
        return preEvent;
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
