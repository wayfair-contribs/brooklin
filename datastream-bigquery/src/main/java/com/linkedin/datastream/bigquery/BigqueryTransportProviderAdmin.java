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
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.bigquery.schema.BigquerySchemaEvolver;
import com.linkedin.datastream.bigquery.schema.BigquerySchemaEvolverFactory;
import com.linkedin.datastream.bigquery.schema.BigquerySchemaEvolverType;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.common.DatastreamSource;
import com.linkedin.datastream.common.DatastreamUtils;
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

    protected static final String METADATA_DATASET_KEY = "dataset";
    protected static final String METADATA_TABLE_SUFFIX_KEY = "tableSuffix";
    protected static final String METADATA_PARTITION_EXPIRATION_DAYS_KEY = "partitionExpirationDays";
    protected static final String METADATA_SCHEMA_EVOLUTION_MODE_KEY = "schemaEvolutionMode";
    protected static final String METADATA_DEAD_LETTER_TABLE_KEY = "deadLetterTable";
    protected static final String METADATA_AUTO_CREATE_TABLE_KEY = "autoCreateTables";
    protected static final String METADATA_LABELS_KEY = "labels";
    protected static final String METADATA_SCHEMA_REGISTRY_LOCATION_KEY = "schemaRegistryLocation";
    protected static final String METADATA_SCHEMA_ID_KEY = "schemaID";
    protected static final String METADATA_RELAX_AVRO_SCHEMA_VALIDATION = "relaxAvroSchemaValidation";

    private static final int DEFAULT_NUMBER_PARTITIONS = 1;

    private final BigqueryBufferedTransportProvider _bufferedTransportProvider;
    private final String _legacyDefaultProjectId;
    private final String _legacyDefaultSchemaRegistryUrl;

    private final BigquerySchemaEvolver _defaultSchemaEvolver;

    private final BigqueryTransportProviderFactory _bigqueryTransportProviderFactory;
    private final BigqueryDatastreamConfigurationFactory _bigqueryDatastreamConfigurationFactory;

    private final Map<String, BigquerySchemaEvolver> _bigquerySchemaEvolverMap;
    private final Map<BigqueryDatastreamDestination, BigqueryDatastreamConfiguration> _datastreamConfigByDestination;
    private final Map<Datastream, BigqueryTransportProvider> _datastreamTransportProvider;
    private final Map<BigqueryTransportProvider, Set<DatastreamTask>> _transportProviderTasks;

    private final Pattern _legacyDatastreamDestinationConnectionStringPattern;

    private final Logger logger = LoggerFactory.getLogger(BigqueryTransportProviderAdmin.class);

    /**
     * Constructor.
     */
    public BigqueryTransportProviderAdmin(final BigqueryBufferedTransportProvider bufferedTransportProvider,
                                          final BigquerySchemaEvolver defaultSchemaEvolver,
                                          final Map<BigqueryDatastreamDestination, BigqueryDatastreamConfiguration> datastreamConfigByDestination,
                                          final Map<String, BigquerySchemaEvolver> bigquerySchemaEvolverMap,
                                          final BigqueryTransportProviderFactory bigqueryTransportProviderFactory,
                                          final BigqueryDatastreamConfigurationFactory bigqueryDatastreamConfigurationFactory,
                                          final String legacyDefaultProjectId,
                                          final String legacyDefaultSchemaRegistryUrl) {
        this._bufferedTransportProvider = bufferedTransportProvider;
        this._defaultSchemaEvolver = defaultSchemaEvolver;
        this._datastreamConfigByDestination = datastreamConfigByDestination;
        this._bigquerySchemaEvolverMap = bigquerySchemaEvolverMap;
        this._bigqueryTransportProviderFactory = bigqueryTransportProviderFactory;
        this._bigqueryDatastreamConfigurationFactory = bigqueryDatastreamConfigurationFactory;

        this._legacyDefaultProjectId = legacyDefaultProjectId;
        this._legacyDefaultSchemaRegistryUrl = legacyDefaultSchemaRegistryUrl;

        _legacyDatastreamDestinationConnectionStringPattern = Pattern.compile("^[^/]+/[^/]+(/[^/]*)?$");

        _datastreamTransportProvider = new ConcurrentHashMap<>();
        _transportProviderTasks = new ConcurrentHashMap<>();
    }


    @Override
    public TransportProvider assignTransportProvider(final DatastreamTask task) {
        // Assume that the task has a single datastream
        final Datastream datastream = task.getDatastreams().get(0);
        // For legacy datastreams, update the destination connection string and late-init the destination name when record is sent
        if (isLegacyDatastreamDestinationConnectionString(datastream.getDestination().getConnectionString())) {
            try {
                updateConnectionStringOnLegacyDatastream(datastream, "*");
            } catch (final DatastreamValidationException e) {
                logger.error("Unable to assign invalid datastream with name: {}", datastream.getName(), e);
                throw new DatastreamRuntimeException("Unable to assign invalid datastream", e);
            }
        }
        return _datastreamTransportProvider.computeIfAbsent(datastream, d -> {
            final BigqueryDatastreamConfiguration configuration = getConfigurationFromDatastream(d);
            final BigqueryTransportProvider transportProvider =  _bigqueryTransportProviderFactory.createTransportProvider(_bufferedTransportProvider,
                    configuration.getValueSerializer(), configuration.getValueDeserializer(), configuration, _datastreamConfigByDestination);
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

        final DatastreamSource source = datastream.getSource();
        final DatastreamDestination destination = datastream.getDestination();

        // Skip the destination partition validation for datastreams that have connector-managed destinations
        // (i.e. mirroring connectors)
        if (!DatastreamUtils.isConnectorManagedDestination(datastream) && (!destination.hasPartitions() || destination.getPartitions() <= 0)) {
            if (source.hasPartitions()) {
                destination.setPartitions(source.getPartitions());
            } else {
                logger.warn("Unable to set the number of partitions in a destination, set to default {}", DEFAULT_NUMBER_PARTITIONS);
                destination.setPartitions(DEFAULT_NUMBER_PARTITIONS);
            }
        }

        if (destination.hasConnectionString() && !isLegacyDatastreamDestinationConnectionString(destination.getConnectionString())) {
            try {
                BigqueryDatastreamDestination.parse(destination.getConnectionString());
            } catch (IllegalArgumentException e) {
                throw new DatastreamValidationException("Bigquery datastream destination is malformed", e);
            }
        } else {
            logger.warn("Updating connection string on legacy datastream with name: {}", datastream.getName());
            updateConnectionStringOnLegacyDatastream(datastream, destinationName);
        }
    }

    private void updateConnectionStringOnLegacyDatastream(final Datastream datastream, final String destinationName) throws DatastreamValidationException {
        final String dataset = datastream.getMetadata().get(METADATA_DATASET_KEY);
        if (StringUtils.isBlank(dataset)) {
            throw new DatastreamValidationException("Metadata dataset is not set in the datastream definition.");
        }

        final BigqueryDatastreamDestination datastreamDestination = new BigqueryDatastreamDestination(
                _legacyDefaultProjectId,
                dataset,
                destinationName + datastream.getMetadata().getOrDefault(METADATA_TABLE_SUFFIX_KEY, "")
        );
        datastream.getDestination().setConnectionString(datastreamDestination.toString());
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

    BigqueryDatastreamConfiguration getConfigurationFromDatastream(final Datastream datastream) {
        final Map<String, String> metadata = datastream.getMetadata();
        final String schemaRegistryLocation = metadata.getOrDefault(METADATA_SCHEMA_REGISTRY_LOCATION_KEY, _legacyDefaultSchemaRegistryUrl);
        final BigquerySchemaEvolver schemaEvolver = Optional.ofNullable(metadata.get(METADATA_SCHEMA_EVOLUTION_MODE_KEY)).map(
                name -> Optional.ofNullable(_bigquerySchemaEvolverMap.get(name))
                        .orElseThrow(() -> new IllegalStateException(String.format("schema evolver not found with name: %s", name))
                        )).orElse(_defaultSchemaEvolver);
        final boolean autoCreateDestinationTable = Optional.ofNullable(metadata.get(METADATA_AUTO_CREATE_TABLE_KEY))
                .map(Boolean::valueOf).orElse(true);
        final Long partitionExpirationDays = Optional.ofNullable(metadata.get(METADATA_PARTITION_EXPIRATION_DAYS_KEY))
                .map(Long::valueOf).orElse(null);

        final BigqueryDatastreamDestination destination = BigqueryDatastreamDestination.parse(datastream.getDestination().getConnectionString());
        final BigqueryDatastreamDestination deadLetterTable = BigqueryDatastreamDestination
                .parse(metadata.getOrDefault(METADATA_DEAD_LETTER_TABLE_KEY, destination.toString() + "_exceptions"));
        final BigqueryDatastreamConfiguration deadLetterTableConfiguration = _bigqueryDatastreamConfigurationFactory.createBigqueryDatastreamConfiguration(
                deadLetterTable,
                datastream.getName(),
                schemaRegistryLocation,
                BigquerySchemaEvolverFactory.createBigquerySchemaEvolver(BigquerySchemaEvolverType.dynamic),
                true,
                null,
                null,
                null,
                null,
                null
        );

        final List<BigqueryLabel> labels = Optional.ofNullable(metadata.get(METADATA_LABELS_KEY))
                .map(this::parseLabelsString).orElse(null);
        final Integer schemaId = Optional.ofNullable(metadata.get(METADATA_SCHEMA_ID_KEY))
                .map(Integer::valueOf).orElse(null);
        final Boolean relaxAvroSchemaValidation = Optional.ofNullable(metadata.get(METADATA_RELAX_AVRO_SCHEMA_VALIDATION))
                .map(Boolean::valueOf).orElse(null);

        return _bigqueryDatastreamConfigurationFactory.createBigqueryDatastreamConfiguration(
                destination,
                datastream.getName(),
                schemaRegistryLocation,
                schemaEvolver,
                autoCreateDestinationTable,
                partitionExpirationDays,
                deadLetterTableConfiguration,
                labels,
                schemaId,
                relaxAvroSchemaValidation
                );
    }

    private boolean isLegacyDatastreamDestinationConnectionString(final String destinationConnectionString) {
        return _legacyDatastreamDestinationConnectionStringPattern.matcher(destinationConnectionString).matches();
    }

    protected List<BigqueryLabel> parseLabelsString(final String labelsString) {
        // Parse to map first to find overlapping label names
        final Map<String, String> labelsMap = Arrays.stream(labelsString.split(","))
                .filter(StringUtils::isNotBlank)
                .map(label -> {
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
