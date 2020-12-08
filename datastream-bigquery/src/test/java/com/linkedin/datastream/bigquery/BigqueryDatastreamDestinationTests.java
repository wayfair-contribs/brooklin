/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.bigquery;

import java.net.URI;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class BigqueryDatastreamDestinationTests {

    @Test
    public void testToUri() {
        final BigqueryDatastreamDestination destination = new BigqueryDatastreamDestination("project_name", "dataset", "*");
        final URI uri = destination.getUri();
        assertEquals(uri.toString(), "brooklin-bigquery://project_name.dataset.*");
    }

    @Test
    public void testParse() {
        final BigqueryDatastreamDestination destination = new BigqueryDatastreamDestination("project_name", "dataset", "*");
        assertEquals(
                BigqueryDatastreamDestination.parse("brooklin-bigquery://project_name.dataset.*"),
                destination
        );
    }

}
