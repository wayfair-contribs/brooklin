/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.bigquery;

import java.net.URI;
import java.util.Objects;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

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

    @Test
    public void testParseDestinationWithPeriod() {
        final BigqueryDatastreamDestination destination = new BigqueryDatastreamDestination("project_name", "dataset", "test.topic.with.period");
        assertEquals(
                BigqueryDatastreamDestination.parse("brooklin-bigquery://project_name.dataset.test.topic.with.period"),
                destination
        );
    }

    @Test
    public void testEquals() {
        final String project = "project";
        final String dataset = "dataset";
        final String destinationName = "destination";
        final BigqueryDatastreamDestination destination = new BigqueryDatastreamDestination(project, dataset, destinationName);
        final String url = destination.toString();

        assertEquals(new BigqueryDatastreamDestination(project, dataset, destinationName), destination);
        assertEquals(BigqueryDatastreamDestination.parse(url), destination);
    }

    @Test
    public void testHashCode() {
        final String project = "project";
        final String dataset = "dataset";
        final String destinationName = "destination";
        final BigqueryDatastreamDestination destination = new BigqueryDatastreamDestination(project, dataset, destinationName);
        final String url = destination.toString();

        assertEquals(new BigqueryDatastreamDestination(project, dataset, destinationName).hashCode(), destination.hashCode());
        assertEquals(BigqueryDatastreamDestination.parse(url).hashCode(), destination.hashCode());
    }

}
