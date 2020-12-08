/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.bigquery;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.Validate;
import org.apache.http.client.utils.URIBuilder;

/**
 * A class to hold Bigquery Datastream destination information.
 */
public class BigqueryDatastreamDestination {
    private final String projectId;
    private final String datasetId;
    private final String destinatonName;
    private final URI uri;

    private static final String SCHEME = "brooklin-bigquery";
    private static final Pattern DESTINATION_PATTERN = Pattern.compile(SCHEME + "://([^.]+)\\.([^.]+)\\.([^.]+)");

    /**
     * Constructor.
     * @param projectId a String
     * @param datasetId a String
     * @param destinationName a String
     */
    public BigqueryDatastreamDestination(final String projectId, final String datasetId, final String destinationName) {
        Validate.notBlank(projectId, "projectId cannot be blank");
        Validate.notBlank(datasetId, "datasetId cannot be blank");
        Validate.notBlank(datasetId, "destinationName cannot be blank");
        this.projectId = projectId;
        this.datasetId = datasetId;
        this.destinatonName = destinationName;
        uri = toUri(this);
    }

    /**
     * Utility function to parse a destination Stringo into a BigqueryDatastreamDestination
     * @param destination a String
     * @return the BigqueryDatastreamDestination
     */
    public static BigqueryDatastreamDestination parse(final String destination) {
        final Matcher destinationMatcher = DESTINATION_PATTERN.matcher(destination);
        Validate.isTrue(destinationMatcher.matches(), "destination is malformed");
        return new BigqueryDatastreamDestination(destinationMatcher.group(1), destinationMatcher.group(2), destinationMatcher.group(3));
    }

    @Override
    public String toString() {
        return uri.toString();
    }

    public URI getUri() {
        return uri;
    }

    private static URI toUri(final BigqueryDatastreamDestination destination) {
        final String host = String.join(".", destination.projectId, destination.datasetId, destination.destinatonName);
        final URIBuilder builder = new URIBuilder()
                .setScheme(SCHEME)
                .setHost(host);
        try {
            return builder.build();
        } catch (final URISyntaxException e) {
            throw new IllegalStateException(e);
        }
    }

    public String getProjectId() {
        return projectId;
    }

    public String getDatasetId() {
        return datasetId;
    }

    public String getDestinatonName() {
        return destinatonName;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        } else if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final BigqueryDatastreamDestination that = (BigqueryDatastreamDestination) o;
        return projectId.equals(that.projectId) && datasetId.equals(that.datasetId) && destinatonName.equals(that.destinatonName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(projectId, datasetId, destinatonName);
    }
}
