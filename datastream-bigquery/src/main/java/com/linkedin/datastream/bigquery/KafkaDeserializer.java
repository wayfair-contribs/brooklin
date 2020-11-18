package com.linkedin.datastream.bigquery;

import com.linkedin.datastream.serde.Deserializer;

public class KafkaDeserializer implements Deserializer {

    private final org.apache.kafka.common.serialization.Deserializer<Object> deserializer;

    public KafkaDeserializer(final org.apache.kafka.common.serialization.Deserializer<Object> deserializer) {
        this.deserializer = deserializer;
    }

    @Override
    public Object deserialize(final byte[] data) {
        return deserializer.deserialize(null, data);
    }
}
