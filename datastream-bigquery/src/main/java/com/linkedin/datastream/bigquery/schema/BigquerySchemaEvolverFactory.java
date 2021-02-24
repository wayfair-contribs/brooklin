/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.bigquery.schema;

/**
 * A factory to construct BigquerySchemaEvolver instances.
 */
public class BigquerySchemaEvolverFactory {

    private static final BigquerySchemaEvolver DYNAMIC_SCHEMA_EVOLVER = new DynamicBigquerySchemaEvolver();
    private static final BigquerySchemaEvolver FIXED_SCHEMA_EVOLVER = new FixedBigquerySchemaEvolver();

    /**
     * Factory method to create a BigquerySchemaEvolver based on the BigquerySchemaEvolverType
     * @param schemaEvolverType a BigquerySchemaEvolverType
     * @return the BigquerySchemaEvolver
     */
    public static BigquerySchemaEvolver createBigquerySchemaEvolver(final BigquerySchemaEvolverType schemaEvolverType) {
        final BigquerySchemaEvolver schemaEvolver;
        switch (schemaEvolverType) {
            case dynamic:
                schemaEvolver = DYNAMIC_SCHEMA_EVOLVER;
                break;
            case fixed:
                schemaEvolver = FIXED_SCHEMA_EVOLVER;
                break;
            default:
                throw new IllegalStateException("Unsupported BigquerySchemaEvolverType: " + schemaEvolverType);
        }
        return schemaEvolver;
    }

}
