/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.bigquery;

/**
 * An implementation of a Serializer for serializing Kafka objects.
 */
public class KafkaSerializer implements Serializer {

    private final org.apache.kafka.common.serialization.Serializer<Object> serializer;


    /**
     * Constructor.
     * @param serializer a org.apache.kafka.common.serialization.Serializer that serializes from an object to a byte array
     */
    public KafkaSerializer(final org.apache.kafka.common.serialization.Serializer<Object> serializer) {
        this.serializer = serializer;
    }

    @Override
    public byte[] serialize(final String topic, final Object data) {
        return serializer.serialize(topic, data);
    }

}
