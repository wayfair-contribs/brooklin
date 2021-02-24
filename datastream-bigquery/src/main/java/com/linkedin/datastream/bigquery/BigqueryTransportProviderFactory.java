/*
 * Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */

package com.linkedin.datastream.bigquery;

import java.util.concurrent.ConcurrentMap;

import com.linkedin.datastream.serde.Deserializer;

/**
 * Factory for BigqueryTransportProvider.
 */
public class BigqueryTransportProviderFactory {

    /**
     * Create a BigqueryTransportProvider.
     * @param bufferedTransportProvider a BigqueryBufferedTransportProvider
     * @param valueSerializer a Serializer
     * @param valueDeserializer a Deserializer
     * @param datastreamConfiguration a BigqueryDatastreamConfiguration
     * @param destinationConfigs a mapping of BigqueryDatastreamDestination to BigqueryDatastreamConfiguration
     * @return the BigqueryTransportProvider
     */
    public BigqueryTransportProvider createTransportProvider(final BigqueryBufferedTransportProvider bufferedTransportProvider,
                                                             final Serializer valueSerializer,
                                                             final Deserializer valueDeserializer,
                                                             final BigqueryDatastreamConfiguration datastreamConfiguration,
                                                             final ConcurrentMap<BigqueryDatastreamDestination, BigqueryDatastreamConfiguration>
                                                                     destinationConfigs) {
        return new BigqueryTransportProvider(bufferedTransportProvider, valueSerializer, valueDeserializer,
                datastreamConfiguration, destinationConfigs);
    }
}
