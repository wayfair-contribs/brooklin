/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.providers;

import java.util.function.Supplier;

/**
 * An abstraction for the connector to maintain information about the progress made
 * in processing {@link com.linkedin.datastream.server.DatastreamTask}s, e.g. checkpoints/offsets.
 * Use the implementation of this interface to support custom checkpointing by the connector.
 *
 * @param <T> checkpoint value type
 */
public interface CustomCheckpointProvider<T extends PersistableCheckpoint,
        S extends PersistableCheckpoint.Deserializer> {

    /**
     * Persist the safe checkpoint to the underlying checkpoint store
     * @param checkpoint safe checkpoint
     */
    void commit(T checkpoint);

    /**
     * close the checkpoint provider
     */
    void close();

    /**
     * get safe checkpoint
     * @return last known safe checkpoint
     */
    T getSafeCheckpoint(Supplier<S> deserSupplier, Class<T> checkpointClass);
}
