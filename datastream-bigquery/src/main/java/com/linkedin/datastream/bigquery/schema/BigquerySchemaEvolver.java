package com.linkedin.datastream.bigquery.schema;

import com.google.cloud.bigquery.Schema;

/**
 * Interface to handle evolving BigQuery schemas.
 */
public interface BigquerySchemaEvolver {

    /**
     * Combine a base and new schema to create an evolved schema.
     * @param baseSchema the Schema to use as the base
     * @param newSchema the Schema to combine with the base
     * @return a Schema representing the evolution of the base and new schemas
     * @throws IncompatibleSchemaEvolutionException if the schemas could not be evolved in a way that is compatible
     */
    Schema evolveSchema(Schema baseSchema, Schema newSchema) throws IncompatibleSchemaEvolutionException;

}
