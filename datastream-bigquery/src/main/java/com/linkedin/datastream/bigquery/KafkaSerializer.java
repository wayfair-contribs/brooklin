package com.linkedin.datastream.bigquery;

public class KafkaSerializer implements Serializer {

    private final org.apache.kafka.common.serialization.Serializer<Object> serializer;


    public KafkaSerializer(final org.apache.kafka.common.serialization.Serializer<Object> serializer) {
        this.serializer = serializer;
    }

    @Override
    public byte[] serialize(final String topic, final Object data) {
        return serializer.serialize(topic, data);
    }

}
