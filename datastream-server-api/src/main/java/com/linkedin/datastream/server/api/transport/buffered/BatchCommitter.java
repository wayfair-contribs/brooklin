package com.linkedin.datastream.server.api.transport.buffered;

import com.linkedin.datastream.common.DatastreamRecordMetadata;
import com.linkedin.datastream.common.SendCallback;

import java.util.List;

public interface BatchCommitter <T> {
    /**
     * Commits the file to cloud storage
     * @param destination destination bucket
     * @param ackCallbacks list of call backs associated to each record in the file that should be called in order
     * @param recordMetadata metadata associated to each record in the file
     * @param callback callback that needs to be called to notify the Object Builder about the completion of the commit
     */
    void commit(List<T> batch,
                String destination,
                List<SendCallback> ackCallbacks,
                List<DatastreamRecordMetadata> recordMetadata,
                CommitCallback callback);

    /**
     * Shutdown the committer
     */
    void shutdown();
}
