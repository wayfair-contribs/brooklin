package com.linkedin.datastream.bigquery.translator;

import com.linkedin.datastream.common.DatastreamRuntimeException;

public class SchemaTranslationException extends DatastreamRuntimeException {
    private static final long serialVersionUID = 1;
    public SchemaTranslationException(final String message) {
        super(message);
    }

}
