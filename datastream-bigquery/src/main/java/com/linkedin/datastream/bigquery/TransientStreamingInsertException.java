/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.bigquery;

import com.linkedin.datastream.common.DatastreamTransientException;

/**
 * A DatastreamTransientException that is thrown when there is an error performing a streaming insert into BigQuery.
 */
public class TransientStreamingInsertException extends DatastreamTransientException {
    private static final long serialVersionUID = 1;

    /**
     * Constructor.
     * @param cause a Throwable
     */
    public TransientStreamingInsertException(final Throwable cause) {
        super(cause);
    }

    /**
     * Constructor.
     * @param message a String
     */
    public TransientStreamingInsertException(final String message) {
        super(message);
    }
}
