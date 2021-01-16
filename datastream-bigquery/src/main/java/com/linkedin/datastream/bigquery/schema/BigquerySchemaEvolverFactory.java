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

    private static final BigquerySchemaEvolver SIMPLE_SCHEMA_EVOLVER = new DynamicBigquerySchemaEvolver();
    private static final BigquerySchemaEvolver NOOP_SCHEMA_EVOLVER = new FixedBigquerySchemaEvolver();

    /**
     * A factory method that constructs a BigquerySchemaEvolver instance from properties.
     * @param properties the VerifiableProperties
     * @return an instance of BigquerySchemaEvolver
     */
    public static BigquerySchemaEvolver createBigquerySchemaEvolver(final VerifiableProperties properties) {
        final String schemaEvolverTypeStr = properties.getString("type", BigquerySchemaEvolverType.fixed.name());
        return createBigquerySchemaEvolver(BigquerySchemaEvolverType.valueOf(schemaEvolverTypeStr));
    }

    /**
     * Factory method to create a BigquerySchemaEvolver based on the BigquerySchemaEvolverType
     * @param schemaEvolverType a BigquerySchemaEvolverType
     * @return the BigquerySchemaEvolver
     */
    public static BigquerySchemaEvolver createBigquerySchemaEvolver(final BigquerySchemaEvolverType schemaEvolverType) {
        final BigquerySchemaEvolver schemaEvolver;
        switch (schemaEvolverType) {
            case dynamic:
                schemaEvolver = SIMPLE_SCHEMA_EVOLVER;
                break;
            case fixed:
                schemaEvolver = NOOP_SCHEMA_EVOLVER;
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
