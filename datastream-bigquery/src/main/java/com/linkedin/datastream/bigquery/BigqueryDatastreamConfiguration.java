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

import com.linkedin.datastream.bigquery.schema.BigquerySchemaEvolver;

/**
 * A class to hold Bigquery Datastream configurations.
 */
public class BigqueryDatastreamConfiguration {

    private final BigquerySchemaEvolver _schemaEvolver;
    private final boolean _manageDestinationTable;
    private final Long _partitionExpirationDays;
    private final String _tableNameTemplate;
    private final BigqueryDatastreamConfiguration _exceptionsTableConfiguration;
    private final List<BigqueryLabel> _labels;

    /**
     * Constructor.
     * @param schemaEvolver the BigquerySchemaEvolver
     * @param manageDestinationTable should transport provider manage the destination table
     * @param partitionExpirationDays optional partition expiration days. Should be greater than zero or null
     * @param tableNameTemplate optional table name template. Should be non-blank or null
     * @param exceptionsTableConfiguration optional BigqueryDatastreamConfiguration
     */
    public BigqueryDatastreamConfiguration(final BigquerySchemaEvolver schemaEvolver, final boolean manageDestinationTable, final Long partitionExpirationDays,
                                           final String tableNameTemplate,
                                           final BigqueryDatastreamConfiguration exceptionsTableConfiguration,
                                           final List<BigqueryLabel> labels) {
        _schemaEvolver = schemaEvolver;
        _manageDestinationTable = manageDestinationTable;
        _partitionExpirationDays = partitionExpirationDays;
        _tableNameTemplate = tableNameTemplate;
        _exceptionsTableConfiguration = exceptionsTableConfiguration;
        if (labels != null) {
            _labels = new ArrayList<>(labels);
        } else {
            _labels = Collections.emptyList();
        }
    }

    /**
     * Constructor.
     * @param schemaEvolver the BigquerySchemaEvolver
     * @param manageDestinationTable should transport provider manage the destination table
     */
    public BigqueryDatastreamConfiguration(final BigquerySchemaEvolver schemaEvolver, final boolean manageDestinationTable) {
        this(schemaEvolver, manageDestinationTable, null, null, null, null);
    }

    public Optional<Long> getPartitionExpirationDays() {
        return Optional.ofNullable(_partitionExpirationDays);
    }

    public BigquerySchemaEvolver getSchemaEvolver() {
        return _schemaEvolver;
    }

    public boolean isManageDestinationTable() {
        return _manageDestinationTable;
    }

    public Optional<String> getTableNameTemplate() {
        return Optional.ofNullable(_tableNameTemplate);
    }

    public Optional<BigqueryDatastreamConfiguration> getExceptionsTableConfiguration() {
        return Optional.ofNullable(_exceptionsTableConfiguration);
    }

    public List<BigqueryLabel> getLabels() {
        return Collections.unmodifiableList(_labels);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        } else if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final BigqueryDatastreamConfiguration that = (BigqueryDatastreamConfiguration) o;
        return _schemaEvolver.equals(that._schemaEvolver) && _manageDestinationTable == that._manageDestinationTable
                && Objects.equals(_partitionExpirationDays, that._partitionExpirationDays)
                && Objects.equals(_tableNameTemplate, that._tableNameTemplate)
                && Objects.equals(_exceptionsTableConfiguration, that._exceptionsTableConfiguration)
                && Objects.equals(_labels, that._labels);
    }

    @Override
    public int hashCode() {
        return Objects.hash(_schemaEvolver, _manageDestinationTable, _partitionExpirationDays, _tableNameTemplate, _exceptionsTableConfiguration, _labels);
    }
}
