/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.bigquery.schema;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.linkedin.datastream.common.VerifiableProperties;

/**
 * A factory to construct BigquerySchemaEvolver instances.
 */
public class BigquerySchemaEvolverFactory {

    /**
     * A factory method that constructs a BigquerySchemaEvolver instance from properties.
     * @param properties the VerifiableProperties
     * @return an instance of BigquerySchemaEvolver
     */
    public static BigquerySchemaEvolver createBigquerySchemaEvolver(final VerifiableProperties properties) {
        final String schemaEvolverTypeStr = properties.getString("type", BigquerySchemaEvolverType.noop.name());
        final BigquerySchemaEvolverType schemaEvolverType = BigquerySchemaEvolverType.valueOf(schemaEvolverTypeStr);
        final BigquerySchemaEvolver schemaEvolver;
        switch (schemaEvolverType) {
            case simple:
                schemaEvolver = new SimpleBigquerySchemaEvolver();
                break;
            case noop:
                schemaEvolver = new NoOpBigquerySchemaEvolver();
                break;
            default:
                throw new IllegalStateException("Unsupported BigquerySchemaEvolverType: " + schemaEvolverType);
        }
        return schemaEvolver;
    }

    /**
     * A factory method that constructs a map of schema evovler names to BigquerySchemaEvolver instance from properties.
     * @param properties the VerifiableProperties
     * @return a map of schema evolver name to BigquerySchemaEvolver
     */
    public static Map<String, BigquerySchemaEvolver> createBigquerySchemaEvolvers(final VerifiableProperties properties) {
        final List<String> names = properties.getStringList("names");
        return names.stream().collect(Collectors.toMap(name -> name, name -> {
            final VerifiableProperties schemaEvolverProperties = new VerifiableProperties(properties.getDomainProperties(name));
            return createBigquerySchemaEvolver(schemaEvolverProperties);
        }));
    }
}
