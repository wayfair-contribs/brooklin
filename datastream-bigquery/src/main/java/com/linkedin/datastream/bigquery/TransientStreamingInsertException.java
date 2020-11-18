package com.linkedin.datastream.bigquery;

import com.linkedin.datastream.common.DatastreamTransientException;

public class TransientStreamingInsertException extends DatastreamTransientException {
    private static final long serialVersionUID = 1;
    public TransientStreamingInsertException(final Throwable cause) {
        super(cause);
    }

    public TransientStreamingInsertException(final String message) {
        super(message);
    }
}
