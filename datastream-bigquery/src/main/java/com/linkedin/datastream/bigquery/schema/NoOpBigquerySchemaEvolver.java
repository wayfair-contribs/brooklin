package com.linkedin.datastream.bigquery.schema;

import com.google.cloud.bigquery.Schema;

/**
 * A NoOp implementation of a BigquerySchemaEvolver.
 */
public class NoOpBigquerySchemaEvolver implements BigquerySchemaEvolver {

    @Override
    public Schema evolveSchema(final Schema baseSchema, final Schema newSchema) throws IncompatibleSchemaEvolutionException {
        return baseSchema;
    }

}
