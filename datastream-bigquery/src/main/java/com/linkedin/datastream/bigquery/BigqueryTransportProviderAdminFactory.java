package com.linkedin.datastream.bigquery;

import com.linkedin.datastream.server.api.transport.TransportProviderAdmin;
import com.linkedin.datastream.server.api.transport.TransportProviderAdminFactory;

import java.util.Properties;

/**
 * Simple Bigquery Transport provider factory that creates one producer for the entire system
 */
public class BigqueryTransportProviderAdminFactory implements TransportProviderAdminFactory {
    @Override
    public TransportProviderAdmin createTransportProviderAdmin(String transportProviderName,
                                                               Properties transportProviderProperties) {
        return new BigqueryTransportProviderAdmin(transportProviderName, transportProviderProperties);
    }
}
