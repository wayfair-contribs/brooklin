/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.bigquery;

import com.linkedin.datastream.serde.Deserializer;

/**
 * An implementation of Deserializer for deserializing Kafka objects.
 */
public class KafkaDeserializer implements Deserializer {

    private final org.apache.kafka.common.serialization.Deserializer<Object> deserializer;

    /**
     * Constructor.
     * @param deserializer a org.apache.kafka.common.serialization.Deserializer that deserializes from a byte array to an object
     */
    public KafkaDeserializer(final org.apache.kafka.common.serialization.Deserializer<Object> deserializer) {
        this.deserializer = deserializer;
    }

    @Override
    public Object deserialize(final byte[] data) {
        return deserializer.deserialize(null, data);
    }
}
