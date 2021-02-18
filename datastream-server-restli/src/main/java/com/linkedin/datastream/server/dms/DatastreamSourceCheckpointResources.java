/**
 *  Copyright 2021 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.dms;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamStatus;
import com.linkedin.datastream.diagnostics.ConnectorHealth;
import com.linkedin.datastream.diagnostics.TaskHealth;
import com.linkedin.datastream.diagnostics.TaskHealthArray;
import com.linkedin.datastream.server.Coordinator;
import com.linkedin.datastream.server.DatastreamServer;
import com.linkedin.datastream.server.ErrorLogger;
import com.linkedin.datastream.server.providers.CustomCheckpointProvider;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.UpdateResponse;
import com.linkedin.restli.server.annotations.RestLiSimpleResource;
import com.linkedin.restli.server.resources.SimpleResourceTemplate;

/**
 * The Restli resource of {@link ConnectorHealth}. Used for collecting datastream checkpoint info
 * about a {@link Datastream}.
 */
@RestLiSimpleResource(name = "checkpoint", namespace = "com.linkedin.datastream.server.dms", parent = DatastreamResources.class)
public class DatastreamSourceCheckpointResources extends SimpleResourceTemplate<ConnectorHealth> {
    private static final Logger LOG = LoggerFactory.getLogger(DatastreamSourceCheckpointResources.class);
    private static final String CONFIG_CHECKPOINT_STORE_URL = "checkpointStoreURL";
    private static final String CONFIG_CHECKPOINT_STORE_TOPIC = "checkpointStoreTopic";
    public final DatastreamServer _server;
    private final DatastreamStore _store;
    private final Coordinator _coordinator;
    private final ErrorLogger _errorLogger;


    /**
     * Construct an instance of DatastreamCheckpointResources
     * @param datastreamServer Datastream server for which health data is retrieved
     */
    public DatastreamSourceCheckpointResources(DatastreamServer datastreamServer) {
        _server = datastreamServer;
        _store = _server.getDatastreamStore();
        _coordinator = _server.getCoordinator();
        _errorLogger = new ErrorLogger(LOG, _coordinator.getInstanceName());
    }

    /**
     * Get source checkpoints of the parent datastream for each tasks
     * @return current checkpoint
     */
    @Override
    public ConnectorHealth get() {
        String datastreamName = getParentKey();
        try {
            Datastream datastream = _store.getDatastream(datastreamName);
            if (datastream == null) {
                _errorLogger.logAndThrowRestLiServiceException(HttpStatus.S_404_NOT_FOUND,
                        "Datastream does not exist: " + datastreamName);

            }
            return buildSourceCheckpoint(datastream);
        } catch (Exception e) {
            _errorLogger.logAndThrowRestLiServiceException(HttpStatus.S_500_INTERNAL_SERVER_ERROR,
                    "Get datastream checkpoint failed for datastream: " + datastreamName, e);
        }
        // Returning null will automatically trigger a 404 Not Found response
        return null;
    }

    /**
     * Update source checkpoint on a given datastream
     * @param input desired checkpoint value
     * @return
     */
    @Override
    public UpdateResponse update(ConnectorHealth input) {
        String datastreamName = getParentKey();
        Datastream datastream = _store.getDatastream(datastreamName);
        if (datastream == null) {
            _errorLogger.logAndThrowRestLiServiceException(HttpStatus.S_404_NOT_FOUND,
                    "Datastream does not exist: " + datastreamName);
        }
        // Currently only KafkaCustomCheckpointProvider supports rewinding the checkpoint
        if (!_server.isCustomCheckpointing(datastream.getConnectorName())) {
            _errorLogger.logAndThrowRestLiServiceException(HttpStatus.S_501_NOT_IMPLEMENTED,
                    "This datastream doesn't support checkpoints to be updated.");
        }
        // Only paused datastreams can be updated
        if (!DatastreamStatus.PAUSED.equals(datastream.getStatus())) {
            _errorLogger.logAndThrowRestLiServiceException(HttpStatus.S_405_METHOD_NOT_ALLOWED,
                    "Datastream must be in PAUSED state before updating checkpoint");
        }
        CustomCheckpointProvider<Long> customCheckpointProvider = null;
        try {
            // Sample SourceCheckpoint resource {"tasks":["sourceCheckpoint":"1234"]}
            final long newCheckpoint = Long.parseLong(input.getTasks().get(0).getSourceCheckpoint());
            customCheckpointProvider = _server.getCustomCheckpointProvider(datastream);
            customCheckpointProvider.rewindTo(newCheckpoint);
        } catch (Exception e) {
            _errorLogger.logAndThrowRestLiServiceException(HttpStatus.S_400_BAD_REQUEST,
                    "Could not complete datastream checkpoint update.", e);
        } finally {
            if (customCheckpointProvider != null) {
                customCheckpointProvider.close();
            }
        }
        return new UpdateResponse(HttpStatus.S_200_OK);
    }

    private ConnectorHealth buildSourceCheckpoint(Datastream datastream) {
        ConnectorHealth sourceCheckpointDetail = new ConnectorHealth();
        sourceCheckpointDetail.setTasks(buildTaskSourceCheckpoint(datastream));

        return sourceCheckpointDetail;
    }

    private TaskHealthArray buildTaskSourceCheckpoint(Datastream datastream) {
        TaskHealthArray tasksSourceCheckpoint = new TaskHealthArray();
        _coordinator.getDatastreamTasks().stream()
                .filter(t -> t.getDatastreams().get(0).getName().equals(datastream.getName())).forEach(task -> {
            TaskHealth taskHealth = new TaskHealth();
            taskHealth.setName(task.getDatastreamTaskName());
            if (_server.isCustomCheckpointing(datastream.getConnectorName())) {
                CustomCheckpointProvider<Long> customCheckpointProvider = null;
                try {
                    customCheckpointProvider = _server.getCustomCheckpointProvider(task.getDatastreams().get(0));
                    taskHealth.setSourceCheckpoint(customCheckpointProvider.getSafeCheckpoint().toString());
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    if (customCheckpointProvider != null) {
                        customCheckpointProvider.close();
                    }
                }
            } else {
                taskHealth.setSourceCheckpoint(task.getCheckpoints().toString());
            }
            tasksSourceCheckpoint.add(taskHealth);
        });

        return tasksSourceCheckpoint;
    }


    private String getParentKey() {
        return getContext().getPathKeys().getAsString(DatastreamResources.KEY_NAME);
    }

}
