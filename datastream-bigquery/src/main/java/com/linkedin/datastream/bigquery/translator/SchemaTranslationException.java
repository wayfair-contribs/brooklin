/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.bigquery.translator;

import com.linkedin.datastream.common.DatastreamRuntimeException;

/**
 * A DatastreamRuntimeException that is thrown when a schema fails to translate.
 */
public class SchemaTranslationException extends DatastreamRuntimeException {
    private static final long serialVersionUID = 1;

    /**
     * Constructor.
     * @param message a String
     */
    public SchemaTranslationException(final String message) {
        super(message);
    }

}
