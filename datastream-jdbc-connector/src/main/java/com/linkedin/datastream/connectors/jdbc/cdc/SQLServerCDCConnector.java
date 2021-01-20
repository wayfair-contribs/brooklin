/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.connectors.jdbc.cdc;

import com.linkedin.data.template.StringMap;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.common.VerifiableProperties;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.api.connector.Connector;
import com.linkedin.datastream.server.api.connector.DatastreamValidationException;
import com.linkedin.datastream.server.providers.CheckpointProvider;
import com.wayfair.crypto.Passwords;
import org.apache.commons.dbcp.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * implementation of {@link Connector}
 * JDBC connector to stream data from SQL tables
 */
public class SQLServerCDCConnector implements Connector {

    private static final Logger LOG = LoggerFactory.getLogger(SQLServerCDCConnector.class);

    private static final String CONFIG_JDBC_USER = "user";
    private static final String CONFIG_JDBC_CREDENTIAL_NAME = "credentialName";
    private static final String CONFIG_JDBC_PASSWORD = "password";
    private static final String CONFIG_CP_MIN_IDLE = "cpMinIdle";
    private static final String CONFIG_CP_MAX_IDLE = "cpMaxIdle";
    private static final String CONFIG_CHECKPOINT_STORE_URL = "checkpointStoreURL";
    private static final String CONFIG_CHECKPOINT_STORE_TOPIC = "checkpointStoreTopic";

    private static final String DS_CONFIG_MAX_TASKS = "maxTasks";
    private static final String DS_CONFIG_TABLE = "table";
    private static final String DS_CONFIG_INCREMENTING_COLUMN_NAME = "incrementingColumnName";
    private static final String DS_CONFIG_INCREMENTING_INITIAL = "incrementingInitial";
    private static final String DS_CONFIG_QUERY = "query";
    private static final String DS_CONFIG_POLL_FREQUENCY_MS = "pollFrequencyMS";
    private static final String DS_CONFIG_MAX_POLL_ROWS = "maxPollRows";
    private static final String DS_CONFIG_MAX_FETCH_SIZE = "maxFetchSize";
    private static final String DS_CONFIG_DESTINATION_TOPIC = "destinationTopic";

    private static final int DEFAULT_MAX_POLL_RECORDS = 10000;
    private static final int DEFAULT_MAX_FETCH_SIZE = 1000;

    private final ConcurrentMap<String, DataSource> _datasources;
    private final Map<DatastreamTask, SQLServerCDCConnectorTask> _runningTasks;

    private final String _jdbcUser;
    private final String _jdbcUserPassword;
    private final int _cpMinIdle;
    private final int _cpMaxIdle;
    private final String _checkpointStoreUrl;
    private final String _checkpointStoreTopic;

    /**
     * constructor for JDBCConnector
     * @param config configuration options
     */
    public SQLServerCDCConnector(VerifiableProperties config) {
        _datasources = new ConcurrentHashMap<>();
        _runningTasks = new HashMap<>();

        if (!config.containsKey(CONFIG_JDBC_USER)
                || !config.containsKey(CONFIG_JDBC_CREDENTIAL_NAME) && !config.containsKey(CONFIG_JDBC_PASSWORD)) {
            throw new RuntimeException("Config options user or password is missing");
        }

        if (!config.containsKey(CONFIG_CHECKPOINT_STORE_URL) || !config.containsKey(CONFIG_CHECKPOINT_STORE_TOPIC)) {
            throw new RuntimeException("Config options checkpointStoreURL or checkpointStoreTopic is missing");
        }

        _checkpointStoreUrl = config.getProperty(CONFIG_CHECKPOINT_STORE_URL);
        _checkpointStoreTopic = config.getProperty(CONFIG_CHECKPOINT_STORE_TOPIC);

        _jdbcUser = config.getString(CONFIG_JDBC_USER);
        _jdbcUserPassword = getJDBCPassword(config);

        _cpMinIdle = config.getInt(CONFIG_CP_MIN_IDLE, 1);
        _cpMaxIdle = config.getInt(CONFIG_CP_MAX_IDLE, 5);
    }

    private String getJDBCPassword(VerifiableProperties config) {
        String password = config.getString(CONFIG_JDBC_PASSWORD);
        if (password != null) {
            return password;
        }

        try {
            password = Passwords.get(config.getString(CONFIG_JDBC_CREDENTIAL_NAME));
            return password;
        } catch (IOException e) {
            LOG.error("Unable to decrypt password.");
            throw new RuntimeException(e);
        }
    }

    @Override
    public void start(CheckpointProvider checkpointProvider) {
        LOG.info("SQLServerCDCConnector started...");
    }

    @Override
    public synchronized void stop() {
        LOG.info("Stopping connector tasks...");
        _runningTasks.forEach((k, v) -> v.stop());
    }

    @Override
    public String getDestinationName(Datastream stream) {
        return stream.getMetadata().get(DS_CONFIG_DESTINATION_TOPIC);
    }

    @Override
    public synchronized void onAssignmentChange(List<DatastreamTask> tasks) {
        LOG.info("onAssignmentChange called with datastream tasks {}", tasks);
        Set<DatastreamTask> unassigned = new HashSet<>(_runningTasks.keySet());
        unassigned.removeAll(tasks);

        // Stop any unassigned tasks
        unassigned.forEach(t -> {
            _runningTasks.get(t).stop();
            _runningTasks.remove(t);
        });

        for (DatastreamTask task : tasks) {
            if (!_runningTasks.containsKey(task)) {
                LOG.info("Creating CDC connector task for " + task);

                String connString = task.getDatastreamSource().getConnectionString();

                DataSource dataSource = _datasources.computeIfAbsent(
                        connString,
                        k -> {
                            BasicDataSource ds = new BasicDataSource();
                            ds.setUrl(connString);
                            ds.setUsername(_jdbcUser);
                            ds.setPassword(_jdbcUserPassword);
                            ds.setMinIdle(_cpMinIdle);
                            ds.setMaxIdle(_cpMaxIdle);
                            return ds;
                        }
                );

                StringMap metadata = task.getDatastreams().get(0).getMetadata();
                SQLServerCDCConnectorTask.SQLServerCDCConnectorTaskBuilder builder = new SQLServerCDCConnectorTask.SQLServerCDCConnectorTaskBuilder();
                builder.setDatastreamName(task.getDatastreams().get(0).getName())
                        .setEventProducer(task.getEventProducer())
                        .setDataSource(dataSource)
                        .setPollFrequencyMS(Long.parseLong(metadata.get(DS_CONFIG_POLL_FREQUENCY_MS)))
                        .setDestinationTopic(metadata.get(DS_CONFIG_DESTINATION_TOPIC))
                        .setCheckpointStoreURL(_checkpointStoreUrl)
                        .setCheckpointStoreTopic(_checkpointStoreTopic);

                if (metadata.containsKey(DS_CONFIG_TABLE)) {
                    builder.setTable(metadata.get(DS_CONFIG_TABLE));
                }

                if (metadata.containsKey(DS_CONFIG_MAX_POLL_ROWS)) {
                    try {
                      builder.setMaxPollRows(Integer.parseInt(metadata.get(DS_CONFIG_MAX_POLL_ROWS)));
                    } catch (NumberFormatException e) {
                        LOG.warn(DS_CONFIG_MAX_POLL_ROWS + " config value is not a valid number. Using the default value " + DEFAULT_MAX_POLL_RECORDS);
                        builder.setMaxPollRows(DEFAULT_MAX_POLL_RECORDS);
                    }
                } else {
                    builder.setMaxPollRows(DEFAULT_MAX_POLL_RECORDS);
                }

                if (metadata.containsKey(DS_CONFIG_MAX_FETCH_SIZE)) {
                    try {
                        builder.setMaxFetchSize(Integer.parseInt(metadata.get(DS_CONFIG_MAX_FETCH_SIZE)));
                    } catch (NumberFormatException e) {
                        LOG.warn(DS_CONFIG_MAX_FETCH_SIZE + " config value is not a valid number. Using the default value " + DEFAULT_MAX_FETCH_SIZE);
                        builder.setMaxFetchSize(DEFAULT_MAX_FETCH_SIZE);
                    }
                } else {
                    builder.setMaxFetchSize(DEFAULT_MAX_FETCH_SIZE);
                }

                SQLServerCDCConnectorTask connectorTask = builder.build();
                _runningTasks.put(task, connectorTask);
                connectorTask.start();
            }
        }
    }

    @Override
    public void initializeDatastream(Datastream stream, List<Datastream> allDatastreams) throws DatastreamValidationException {
        StringMap metadata = stream.getMetadata();
        if (metadata.containsKey(DS_CONFIG_MAX_TASKS)) {
            if (Integer.parseInt(stream.getMetadata().get(DS_CONFIG_MAX_TASKS)) != 1) {
                throw new DatastreamValidationException("maxTasks other than value 1 is not supported.");
            }

        } else {
            metadata.put(DS_CONFIG_MAX_TASKS, "1");
        }

        for (String param : Arrays.asList(DS_CONFIG_POLL_FREQUENCY_MS,
                DS_CONFIG_DESTINATION_TOPIC)) {
            if (!metadata.containsKey(param)) {
                throw new DatastreamValidationException(param + " is missing in the config");
            }
        }

        if ( !metadata.containsKey(DS_CONFIG_TABLE) || metadata.get(DS_CONFIG_TABLE).length() == 0 ) {
            throw new DatastreamValidationException("One of the two config options " + DS_CONFIG_QUERY + " and " + DS_CONFIG_TABLE + " must be provided.");
        }

        metadata.put(DatastreamMetadataConstants.IS_CONNECTOR_MANAGED_DESTINATION_KEY, Boolean.TRUE.toString());

        stream.setMetadata(metadata);
    }

}