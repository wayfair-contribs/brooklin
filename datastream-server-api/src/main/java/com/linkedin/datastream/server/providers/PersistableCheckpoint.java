package com.linkedin.datastream.server.providers;

public interface PersistableCheckpoint {
    /**
     * Serialize a checkpoint into String representation so the checkpoint could be stored into somewhere like Kafka
     * @return
     */
    byte[] serialize();

    /**
     * Deserializer of Checkpoint
     */
    interface Deserializer {

        /**
         * Deserialize a value represented as String and return an instance of PersistableCheckpoint
         * @param value
         * @param <T>
         * @return
         */
        <T extends PersistableCheckpoint> T deserialize(byte[] value, Class<T> checkpointClass);
    }
}
