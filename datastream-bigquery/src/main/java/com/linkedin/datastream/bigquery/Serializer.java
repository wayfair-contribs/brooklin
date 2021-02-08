/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.bigquery;

/**
 * Serializer interface for Brooklin which is used to convert an object to bytes.
 */
public interface Serializer {

    /**
     * Serialize an object for a given topic to a byte array.
     * @param topic a String
     * @param data an Object to serialize
     * @return a byte array
     */
    byte[] serialize(final String topic, final Object data);

}
