/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.bigquery;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import com.linkedin.datastream.bigquery.schema.BigquerySchemaEvolver;
import com.linkedin.datastream.bigquery.schema.BigquerySchemaEvolverFactory;
import com.linkedin.datastream.bigquery.schema.BigquerySchemaEvolverType;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.serde.Deserializer;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.Pair;
import com.linkedin.datastream.server.api.connector.DatastreamValidationException;
import com.linkedin.datastream.server.api.transport.TransportProvider;
import com.linkedin.datastream.server.api.transport.TransportProviderAdmin;


/**
 * {@link TransportProviderAdmin} implementation for {@link BigqueryTransportProvider}
 *
 * <ul>
 *  <li>Initializes {@link BigqueryTransportProvider}</li>
 *  <li>Sets up the correct destination connection string/bq table</li>
 * </ul>
 */
public class BigqueryTransportProviderAdmin implements TransportProviderAdmin {

    protected static final String METADATA_PROJECT_ID_KEY = "projectId";
    protected static final String METADATA_DATASET_KEY = "dataset";
    protected static final String METADATA_TABLE_NAME_TEMPLATE_KEY = "tableNameTemplate";
    protected static final String METADATA_TABLE_SUFFIX_KEY = "tableNameSuffix";
    protected static final String METADATA_PARTITION_EXPIRATION_DAYS_KEY = "partitionExpirationDays";
    protected static final String METADATA_SCHEMA_EVOLVER_KEY = "schemaEvolver";
    protected static final String METADATA_EXCEPTIONS_TABLE_ENABLED_KEY = "exceptionsTableEnabled";
    protected static final String METADATA_MANAGE_DESTINATION_TABLE_KEY = "manageDestinationTable";
    protected static final String METADATA_LABELS_KEY = "labels";

    private final BigqueryBufferedTransportProvider _bufferedTransportProvider;
    private final Serializer _valueSerializer;
    private final Deserializer _valueDeserializer;
    private final Map<String, String> _defaultMetadata;

    private final BigquerySchemaEvolver _defaultSchemaEvolver;

    private final BigqueryTransportProviderFactory _bigqueryTransportProviderFactory;

    private final Map<String, BigquerySchemaEvolver> _bigquerySchemaEvolverMap;
    private final Map<BigqueryDatastreamDestination, BigqueryDatastreamConfiguration> _datastreamConfigByDestination;
    private final Map<Datastream, BigqueryTransportProvider> _datastreamTransportProvider;
    private final Map<BigqueryTransportProvider, Set<DatastreamTask>> _transportProviderTasks;

    /**
     * Constructor.
     */
    public BigqueryTransportProviderAdmin(final BigqueryBufferedTransportProvider bufferedTransportProvider, final Serializer valueSerializer,
                                          final Deserializer valueDeserializer,
                                          final Map<String, String> defaultMetadata, final BigquerySchemaEvolver defaultSchemaEvolver,
                                          final Map<BigqueryDatastreamDestination, BigqueryDatastreamConfiguration> datastreamConfigByDestination,
                                          final Map<String, BigquerySchemaEvolver> bigquerySchemaEvolverMap,
                                          final BigqueryTransportProviderFactory bigqueryTransportProviderFactory) {
        this._bufferedTransportProvider = bufferedTransportProvider;
        this._valueSerializer = valueSerializer;
        this._valueDeserializer = valueDeserializer;
        this._defaultMetadata = defaultMetadata;
        this._defaultSchemaEvolver = defaultSchemaEvolver;
        this._datastreamConfigByDestination = datastreamConfigByDestination;
        this._bigquerySchemaEvolverMap = bigquerySchemaEvolverMap;
        this._bigqueryTransportProviderFactory = bigqueryTransportProviderFactory;

        _datastreamTransportProvider = new ConcurrentHashMap<>();
        _transportProviderTasks = new ConcurrentHashMap<>();
    }


    @Override
    public TransportProvider assignTransportProvider(final DatastreamTask task) {
        // Assume that the task has a single datastream
        final Datastream datastream = task.getDatastreams().get(0);
        return _datastreamTransportProvider.computeIfAbsent(datastream, d -> {
            final BigqueryDatastreamConfiguration configuration = getConfigurationFromDatastream(d);
            final BigqueryTransportProvider transportProvider =  _bigqueryTransportProviderFactory.createTransportProvider(_bufferedTransportProvider,
                    _valueSerializer, _valueDeserializer, configuration, _datastreamConfigByDestination);
            _transportProviderTasks.computeIfAbsent(transportProvider, tp -> ConcurrentHashMap.newKeySet()).add(task);
            return transportProvider;
        });
    }

    Map<Datastream, BigqueryTransportProvider> getDatastreamTransportProviders() {
        return Collections.unmodifiableMap(_datastreamTransportProvider);
    }

    Map<BigqueryTransportProvider, Set<DatastreamTask>> getTransportProviderTasks() {
        return Collections.unmodifiableMap(_transportProviderTasks);
    }

    @Override
    public void unassignTransportProvider(final DatastreamTask task) {
        // Assume that the task has a single datastream
        final Datastream datastream = task.getDatastreams().get(0);
        _datastreamTransportProvider.computeIfPresent(datastream, (d, transportProvider) ->
            Optional.ofNullable(_transportProviderTasks.computeIfPresent(transportProvider, (tp, tasks) -> {
                tasks.remove(task);
                return tasks.isEmpty() ? null : tasks;
            })).map(s -> transportProvider).orElseGet(() -> {
                transportProvider.getDestinations().forEach(_datastreamConfigByDestination::remove);
                transportProvider.close();
                return null;
            })
        );
    }

    @Override
    public void initializeDestinationForDatastream(Datastream datastream, String destinationName)
            throws DatastreamValidationException {
        if (!datastream.hasDestination()) {
            datastream.setDestination(new DatastreamDestination());
        }

        if (datastream.getDestination().hasConnectionString()) {
            try {
                BigqueryDatastreamDestination.parse(datastream.getDestination().getConnectionString());
            } catch (IllegalArgumentException e) {
                throw new DatastreamValidationException("Bigquery datastream destination is malformed", e);
            }
        } else {
            final String projectId = datastream.getMetadata().getOrDefault(METADATA_PROJECT_ID_KEY, _defaultMetadata.get(METADATA_PROJECT_ID_KEY));
            final String dataset = datastream.getMetadata().getOrDefault(METADATA_DATASET_KEY, _defaultMetadata.get(METADATA_DATASET_KEY));
            if (StringUtils.isBlank(dataset)) {
                throw new DatastreamValidationException("Metadata dataset is not set in the datastream definition.");
            }

            final BigqueryDatastreamDestination datastreamDestination = new BigqueryDatastreamDestination(
                    projectId,
                    dataset,
                    destinationName
            );
            datastream.getDestination().setConnectionString(datastreamDestination.toString());
        }
    }

    @Override
    public void createDestination(Datastream datastream) {
    }

    @Override
    public void dropDestination(Datastream datastream) {
    }

    @Override
    public Duration getRetention(Datastream datastream) {
        return Duration.ofSeconds(0);
    }

    BigqueryDatastreamConfiguration getConfigurationFromDatastreamTask(final DatastreamTask task) {
        // Assume that the task has a single datastream
        final Datastream datastream = task.getDatastreams().get(0);
        return getConfigurationFromDatastream(datastream);
    }

    BigqueryDatastreamConfiguration getConfigurationFromDatastream(final Datastream datastream) {
        final Map<String, String> metadata = datastream.getMetadata();
        return new BigqueryDatastreamConfiguration(
                Optional.ofNullable(metadata.get(METADATA_SCHEMA_EVOLVER_KEY)).map(
                        name -> Optional.ofNullable(_bigquerySchemaEvolverMap.get(name))
                                .orElseThrow(() -> new IllegalStateException(String.format("schema evolver not found with name: %s", name))
                                )).orElse(_defaultSchemaEvolver),
                Optional.ofNullable(metadata.getOrDefault(METADATA_MANAGE_DESTINATION_TABLE_KEY, _defaultMetadata.get(METADATA_MANAGE_DESTINATION_TABLE_KEY)))
                        .map(Boolean::valueOf).orElse(true),
                Optional.ofNullable(metadata.getOrDefault(METADATA_PARTITION_EXPIRATION_DAYS_KEY, _defaultMetadata.get(METADATA_PARTITION_EXPIRATION_DAYS_KEY)))
                        .map(Long::valueOf).orElse(null),
                Optional.ofNullable(metadata.get(METADATA_TABLE_NAME_TEMPLATE_KEY))
                        .orElse(Optional.ofNullable(metadata.get(METADATA_TABLE_SUFFIX_KEY))
                                .map(suffix -> "%s" + suffix).orElse(null)),
                Optional.ofNullable(metadata.getOrDefault(METADATA_EXCEPTIONS_TABLE_ENABLED_KEY,
                        _defaultMetadata.get(METADATA_EXCEPTIONS_TABLE_ENABLED_KEY))).filter(Boolean::parseBoolean)
                        .map(enabled -> new BigqueryDatastreamConfiguration(
                                BigquerySchemaEvolverFactory.createBigquerySchemaEvolver(BigquerySchemaEvolverType.simple), true
                                )).orElse(null),
                Optional.ofNullable(metadata.getOrDefault(METADATA_LABELS_KEY, _defaultMetadata.get(METADATA_LABELS_KEY)))
                        .map(this::parseLabelsString).orElse(null)
        );
    }

    private List<BigqueryLabel> parseLabelsString(final String labelsString) {
        // Parse to map first to find overlapping label names
        final Map<String, String> labelsMap = Arrays.stream(labelsString.split(",")).map(label -> {
            final String[] parts = label.split(":");
            if (parts.length == 0 || parts.length > 2) {
                throw new IllegalArgumentException("invalid label: " + label);
            }
            final String name = parts[0];
            final String value;
            if (parts.length == 2) {
                value = parts[1];
            } else {
                value = "";
            }
            return Pair.of(name, value);
        }).collect(Collectors.toMap(Pair::getKey, Pair::getValue));
        return labelsMap.entrySet().stream().map(entry -> new BigqueryLabel(entry.getKey(), entry.getValue())).collect(Collectors.toList());
    }

}
