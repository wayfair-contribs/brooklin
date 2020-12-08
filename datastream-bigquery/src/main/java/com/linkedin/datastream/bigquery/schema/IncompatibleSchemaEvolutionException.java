package com.linkedin.datastream.bigquery.schema;

/**
 * An exception indicating that a requested schema evolution was incompatible.
 */
public class IncompatibleSchemaEvolutionException extends RuntimeException {

    private static final long serialVersionUID = -1;

    /**
     * Constructor.
     * @param message the message
     */
    public IncompatibleSchemaEvolutionException(final String message) {
        super(message);
    }

}
