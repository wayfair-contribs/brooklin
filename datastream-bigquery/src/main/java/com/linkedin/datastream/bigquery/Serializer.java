package com.linkedin.datastream.bigquery;

/**
 * Serializer interface for Brooklin which is used to convert an object to bytes.
 */
public interface Serializer {

    byte[] serialize(final String topic, final Object data);

}
