/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.connectors.jdbc.cdc;

import com.linkedin.datastream.common.VerifiableProperties;
import com.linkedin.datastream.server.api.connector.ConnectorFactory;

import java.util.Properties;

/**
 * implementation of {@link ConnectorFactory} for {@link SQLServerCDCConnector}
 */
public class SQLServerCDCConnectorFactory implements ConnectorFactory<SQLServerCDCConnector>  {
    @Override
    public SQLServerCDCConnector createConnector(String connectorName, Properties config, String clusterName) {
        return new SQLServerCDCConnector(new VerifiableProperties(config));
    }
}
