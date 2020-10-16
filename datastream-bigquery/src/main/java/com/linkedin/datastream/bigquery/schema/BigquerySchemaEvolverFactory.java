package com.linkedin.datastream.bigquery.schema;

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
}
