/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.bigquery.schema;

import com.linkedin.datastream.common.DatastreamRuntimeException;

/**
 * An exception indicating that a requested schema evolution was incompatible.
 */
public class IncompatibleSchemaEvolutionException extends DatastreamRuntimeException {

    private static final long serialVersionUID = -1;

    /**
     * Constructor.
     * @param message the message
     */
    public IncompatibleSchemaEvolutionException(final String message) {
        super(message);
    }

}
