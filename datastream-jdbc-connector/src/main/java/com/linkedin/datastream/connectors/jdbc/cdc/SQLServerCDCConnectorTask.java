/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.connectors.jdbc.cdc;

import com.linkedin.datastream.common.BrooklinEnvelope;
import com.linkedin.datastream.common.BrooklinEnvelopeMetadataConstants;
import com.linkedin.datastream.common.DatastreamRecordMetadata;
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

    private static final String PLACE_HOLDER = "#";
    private static final String GET_MIN_LSN = "SELECT sys.fn_cdc_get_min_lsn('#')";
    private static final String GET_MAX_LSN = "SELECT sys.fn_cdc_get_max_lsn()";

    private static final String INCREMENT_LSN = "SELECT sys.fn_cdc_increment_lsn(?)";
    private static final String GET_ALL_CHANGES_FOR_TABLE = "SELECT * FROM cdc.[fn_cdc_get_all_changes_#](?, ?, N'all update old')";
    private static final String GET_ALL_CHANGES_AFTER = "SELECT * FROM cdc.[fn_cdc_get_all_changes_#](?, ?, N'all')";

    private String _cdc_query;
    private String _min_lsn_query;

    // todo: should be persistent
    private CDCCheckPoint _checkpoint;

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
                        LOG.warn("Failed poll.", e);
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
            _checkpoint = safeCheckpoint.get();
            if (_resetToSafeCommit.get()) {
                // todo: should call rewind ??
                _resetToSafeCommit.set(false);
            }
        }
    }

    protected void execute() {
        LOG.info("CDC poll initiated for task {}", _id);

        mayCommitCheckpoint();

        try (Connection conn = _dataSource.getConnection()) {
            initCheckpointIfNeeded(conn);

            byte[] fromLsn = _checkpoint.lsnBytes();
            byte[] toLsn = getMaxLsn(conn);
            LOG.info("CDC polling parameters: table = {}, checkpoint = {}, from_lsn = {}, to_lsn = {}",
                    _table,
                    _checkpoint,
                    Utils.bytesToHex(fromLsn),
                    Utils.bytesToHex(toLsn) );

            if (fromLsn == null || toLsn == null) {
                LOG.info("Either from or to LSN is not available. Quitting polling");
                return;
            }

            captureChanges(conn, fromLsn, toLsn);

            LOG.info("CDC poll ends for task {}", _id);
        } catch (SQLException e) {
            LOG.error("Failed to poll for CDC table {}", _table, e);
        }
    }

    private void initCheckpointIfNeeded(Connection conn) {
        if (_checkpoint == null) {
            _checkpoint = new CDCCheckPoint(_table, getMinLSN(conn), null);
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

    private byte[] getMaxLsn(Connection conn) {
        // getToLsn: from < to ? to (use old) : getMaxLsn
        try (PreparedStatement preparedStatement = conn.prepareStatement(GET_MAX_LSN) ){

            try (ResultSet rs = preparedStatement.executeQuery()) {
                return rs.next()
                        ? rs.getBytes(1)
                        : null;
            }
        } catch (SQLException e) {
            LOG.error("Failed to get ToLsn when calling {}", INCREMENT_LSN, e);
        }

        return null;
    }

    private Lsn getIncrementalLsn(Connection conn, Lsn checkpoint) {
        try (PreparedStatement preparedStatement = conn.prepareStatement(INCREMENT_LSN)) {
            preparedStatement.setBytes(1, checkpoint.getBytes());

            try (ResultSet rs = preparedStatement.executeQuery()) {
                return rs.next() ? Lsn.valueOf(rs.getBytes(1))
                        : Lsn.NULL;
            }
        } catch (SQLException e) {
            LOG.error("Failed to get FromLsn. The query is {}", INCREMENT_LSN, e);
        }
        return Lsn.NULL;
    }

    private void captureChanges(Connection conn, byte[] fromLsn, byte[] toLsn) {
        try (PreparedStatement preparedStatement = conn.prepareStatement(_cdc_query)) {

            preparedStatement.setBytes(1, fromLsn);
            preparedStatement.setBytes(2, toLsn);
            preparedStatement.setMaxRows(_maxPollRows);
            preparedStatement.setFetchSize(_maxFetchSize);

            try (ResultSet rs = preparedStatement.executeQuery()) {
                translateAndSend(rs);
            }
        } catch (SQLException e) {
            LOG.error("Failed to run cdc functions. The query is {}", _cdc_query, e);
        }
    }

    private void generateQuery() {
        // todo: support more modes

        // _cdc_query = GET_ALL_CHANGES_AFTER.replace(PLACE_HOLDER, _table);
        _cdc_query = GET_ALL_CHANGES_FOR_TABLE.replace(PLACE_HOLDER, _table);

        _min_lsn_query = GET_MIN_LSN.replace(PLACE_HOLDER, _table);
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

    private void translateAndSend(ResultSet resultSet) throws SQLException {

        int resultSetSize = 0;

        Schema avroSchema = getSchema(resultSet.getMetaData());

        while (getNext(resultSet)) {

            long startTime = System.currentTimeMillis();
            resultSetSize++;
            try {
                ChangeEvent event = readChangeEvent(resultSet);
                if (event == null) {
                    // the only case where event is null is that data after is not in ResultSet
                    continue;
                }

                // Skip this event if processed last round of poll
                if (isProcessedEvent(event)) {
                    LOG.trace("The event [ lsn = {}, seq = {} ] is skipped",
                            Utils.bytesToHex(event.lsn), Utils.bytesToHex(event.seq));
                    continue;
                }

                LOG.trace("Translating and sending the event lsn = {}, seq = {}", Utils.bytesToHex(event.lsn), Utils.bytesToHex(event.seq));

                GenericRecord record = AvroRecordTranslator.fromChangeEvent(avroSchema, event);

                CDCCheckPoint checkpoint = new CDCCheckPoint(_table, event.lsn, event.seq);

                HashMap<String, String> meta = new HashMap<>();
                meta.put(BrooklinEnvelopeMetadataConstants.EVENT_TIMESTAMP, String.valueOf(System.currentTimeMillis()));

                BrooklinEnvelope envelope = new BrooklinEnvelope(checkpoint.toBytes(), record, null, meta);

                DatastreamProducerRecordBuilder builder = new DatastreamProducerRecordBuilder();
                builder.addEvent(envelope);
                builder.setEventsSourceTimestamp(System.currentTimeMillis());
                DatastreamProducerRecord producerRecord = builder.build();

                DynamicMetricsManager.getInstance().createOrUpdateHistogram(
                        this.getClass().getSimpleName(), "translate", "exec_time",
                        System.currentTimeMillis() - startTime);

                _flushlessProducer.send(producerRecord, _id, 0, checkpoint,
                        (DatastreamRecordMetadata metadata, Exception exception) -> {
                            if (exception == null) {
                                LOG.trace("sent record {} successfully", checkpoint);
                            } else  {
                                _resetToSafeCommit.set(true);
                                LOG.warn("failed to send record {}.", checkpoint, exception);
                            }
                        });

                // todo
                // _checkpointProvider.updateCheckpoint(checkpoint);
                _checkpoint = checkpoint;
            } catch (SQLException e) {
                // todo: better error handling
                LOG.warn("Error in current row.", e);
            }
        }
        LOG.debug("handled {} cdc events", resultSetSize);
    }

    private Schema getSchema(ResultSetMetaData metaData) {
        // Use cached schema or parse from ResultSet
        if (_avroSchema == null) {
            _avroSchema = SchemaTranslator.fromResultSet(metaData);
        }
        return _avroSchema;
    }

    private boolean isProcessedEvent(ChangeEvent event) {
        return _checkpoint.compare(event.lsn, event.seq) >= 0;
    }

    private ChangeEvent readChangeEvent(ResultSet row) throws SQLException {

        int op = row.getInt(3);
        ChangeEvent event = op == 3
                ? readEventForUpdate(row)
                : readEventForInsertOrDelete(row);
        LOG.debug("Get a change event {}", event);
        return event;
    }

    private ChangeEvent readEventForInsertOrDelete(ResultSet row) throws SQLException {
        int numCols = row.getMetaData().getColumnCount();

        byte[] lsn = row.getBytes(1);
        byte[] seq = row.getBytes(2);
        int op = row.getInt(3);

        int size = numCols - 4;
        Object[] dataAfter = new Object[size];  // exclude 4 meta columns
        //  start from 5 as resultset starting from 1.
        for (int i = 5; i <= numCols; i++) {
            dataAfter[i - 5] = row.getObject(i);
        }

        // todo: code clean
        ChangeEvent event = new ChangeEvent(lsn, seq, op);
        event.dataAfter = dataAfter;

        return event;
    }

    private ChangeEvent readEventForUpdate(ResultSet row) throws SQLException {
        int numCols = row.getMetaData().getColumnCount();

        // read data before from first row
        byte[] lsn = row.getBytes(1);
        byte[] seq = row.getBytes(2);

        int size = numCols - 4;
        Object[] dataBefore = new Object[size];  // exclude 4 meta columns
        //  start from 5 as resultset starting from 1.
        for (int i = 5; i <= numCols; i++) {
            dataBefore[i - 5] = row.getObject(i);
        }

        // try to read after from second row
        if (!row.next()) {
            LOG.debug("The after row is not available for update event");
            return null;
        }

        Object[] dataAfter = new Object[size];  // exclude 4 meta columns
        //  start from 5 as resultset starting from 1.
        for (int i = 5; i <= numCols; i++) {
            dataAfter[i - 5] = row.getObject(i);
        }

        // todo: code clean
        ChangeEvent event = new ChangeEvent(lsn, seq, 3);
        event.dataBefore = dataBefore;
        event.dataAfter = dataAfter;

        return event;
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
