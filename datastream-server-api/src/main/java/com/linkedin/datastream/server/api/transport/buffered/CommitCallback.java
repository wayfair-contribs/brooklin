package com.linkedin.datastream.server.api.transport.buffered;

public interface CommitCallback {
    /**
     * Callback method that needs to be called when the object commit completes
     */
    void commited();
}
