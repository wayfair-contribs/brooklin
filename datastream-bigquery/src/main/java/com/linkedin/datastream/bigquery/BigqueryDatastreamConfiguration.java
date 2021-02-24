/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.bigquery;


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.apache.avro.Schema;

import com.linkedin.datastream.bigquery.schema.BigquerySchemaEvolver;
import com.linkedin.datastream.bigquery.schema.FixedBigquerySchemaEvolver;
import com.linkedin.datastream.serde.Deserializer;

/**
 * A class to hold Bigquery Datastream configurations.
 */
public class BigqueryDatastreamConfiguration {

    private final BigqueryDatastreamDestination _destination;
    private final BigquerySchemaEvolver _schemaEvolver;
    private final boolean _createDestinationTable;
    private final Deserializer _valueDeserializer;
    private final Serializer _valueSerializer;
    private final Long _partitionExpirationDays;
    private final BigqueryDatastreamConfiguration _deadLetterTableConfiguration;
    private final List<BigqueryLabel> _labels;
    private final Schema _fixedSchema;

    /**
     * Constructor.
     * @param builder the Builder
     */
    private BigqueryDatastreamConfiguration(final Builder builder) {
        if (builder._schemaEvolver instanceof FixedBigquerySchemaEvolver && builder._fixedSchema == null) {
            throw new IllegalArgumentException("fixedSchema must not be null when schemaEvolver type is FixedBigquerySchemaEvolver");
        }
        _destination = builder._destination;
        _schemaEvolver = builder._schemaEvolver;
        _createDestinationTable = builder._createDestinationTable;
        _partitionExpirationDays = builder._partitionExpirationDays;
        _deadLetterTableConfiguration = builder._deadLetterTableConfiguration;
        if (builder._labels != null) {
            _labels = new ArrayList<>(builder._labels);
        } else {
            _labels = Collections.emptyList();
        }
        _valueDeserializer = builder._valueDeserializer;
        _valueSerializer = builder._valueSerializer;
        _fixedSchema = builder._fixedSchema;
    }

    public BigqueryDatastreamDestination getDestination() {
        return _destination;
    }

    public Optional<Long> getPartitionExpirationDays() {
        return Optional.ofNullable(_partitionExpirationDays);
    }

    public BigquerySchemaEvolver getSchemaEvolver() {
        return _schemaEvolver;
    }

    public boolean isCreateDestinationTableEnabled() {
        return _createDestinationTable;
    }

    public Deserializer getValueDeserializer() {
        return _valueDeserializer;
    }

    public Serializer getValueSerializer() {
        return _valueSerializer;
    }

    public Optional<BigqueryDatastreamConfiguration> getDeadLetterTableConfiguration() {
        return Optional.ofNullable(_deadLetterTableConfiguration);
    }

    public List<BigqueryLabel> getLabels() {
        return Collections.unmodifiableList(_labels);
    }

    public Optional<Schema> getFixedSchema() {
        return Optional.ofNullable(_fixedSchema);
    }

    /**
     * Builder for BigqueryDatastreamConfiguration.
     */
    public static class Builder {
        private final BigqueryDatastreamDestination _destination;
        private final BigquerySchemaEvolver _schemaEvolver;
        private final boolean _createDestinationTable;
        private final Deserializer _valueDeserializer;
        private final Serializer _valueSerializer;

        private Long _partitionExpirationDays;
        private BigqueryDatastreamConfiguration _deadLetterTableConfiguration;
        private List<BigqueryLabel> _labels;
        private Schema _fixedSchema;

        /**
         * Constructor.
         * @param schemaEvolver the BigquerySchemaEvolver
         * @param createDestinationTable a boolean
         * @param valueDeserializer a value Deserializer
         * @param valueSerializer a value Serializer
         */
        public Builder(final BigqueryDatastreamDestination destination, final BigquerySchemaEvolver schemaEvolver,
                       final boolean createDestinationTable,
                       final Deserializer valueDeserializer, final Serializer valueSerializer
                       ) {
            _schemaEvolver = schemaEvolver;
            _createDestinationTable = createDestinationTable;
            _valueDeserializer = valueDeserializer;
            _valueSerializer = valueSerializer;
            _destination = destination;

            _deadLetterTableConfiguration = null;
            _partitionExpirationDays = null;
            _labels = null;
            _fixedSchema = null;
        }

        /**
         * Set partition expiration days.
         * @param partitionExpirationDays a long value
         * @return the Builder
         */
        public Builder withPartitionExpirationDays(final long partitionExpirationDays) {
            _partitionExpirationDays = partitionExpirationDays;
            return this;
        }

        /**
         * Set labels list.
         * @param labels a List of BigqueryLabel objects
         * @return the Builder
         */
        public Builder withLabels(final List<BigqueryLabel> labels) {
            _labels = labels;
            return this;
        }

        /**
         * Set the fixed schema.
         * @param fixedSchema an Avro Schema
         * @return the Builder
         */
        public Builder withFixedSchema(final Schema fixedSchema) {
            _fixedSchema = fixedSchema;
            return this;
        }

        /**
         * Set the dead letter table configuration.
         * @param deadLetterTableConfiguration the BigqueryDatastreamConfiguration
         * @return the Builder
         */
        public Builder withDeadLetterTableConfiguration(final BigqueryDatastreamConfiguration deadLetterTableConfiguration) {
            _deadLetterTableConfiguration = deadLetterTableConfiguration;
            return this;
        }

        /**
         * Build the BigqueryDatastreamConfiguration.
         * @return the BigqueryDatastreamConfiguration
         */
        public BigqueryDatastreamConfiguration build() {
            return new BigqueryDatastreamConfiguration(this);
        }
    }

    /**
     * Create a new Builder.
     * @param destination a BigqueryDatastreamDestination
     * @param schemaEvolver a BigquerySchemaEvolver
     * @param createDestinationTable a boolean
     * @param valueDeserializer a value Deserializer
     * @param valueSerializer a value Serializer
     * @return the Builder
     */
    public static Builder builder(final BigqueryDatastreamDestination destination,
                                  final BigquerySchemaEvolver schemaEvolver, final boolean createDestinationTable,
                                  final Deserializer valueDeserializer, final Serializer valueSerializer) {
        return new Builder(destination, schemaEvolver, createDestinationTable, valueDeserializer, valueSerializer);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        } else if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final BigqueryDatastreamConfiguration that = (BigqueryDatastreamConfiguration) o;
        return _schemaEvolver.equals(that._schemaEvolver) && _createDestinationTable == that._createDestinationTable
                && _valueDeserializer.equals(that._valueDeserializer)
                && _valueSerializer.equals(that._valueSerializer)
                && _destination.equals(that._destination)
                && Objects.equals(_deadLetterTableConfiguration, that._deadLetterTableConfiguration)
                && Objects.equals(_partitionExpirationDays, that._partitionExpirationDays)
                && Objects.equals(_labels, that._labels)
                && Objects.equals(_fixedSchema, that._fixedSchema);
    }

    @Override
    public int hashCode() {
        return Objects.hash(_schemaEvolver, _createDestinationTable, _valueDeserializer, _valueSerializer,
                _destination, _deadLetterTableConfiguration, _partitionExpirationDays, _labels, _fixedSchema);
    }
}
