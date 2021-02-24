/*
 * Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */

package com.linkedin.datastream.bigquery;

/**
 * A class that holds Schema Registry Client configuration constants.
 */
public class BigquerySchemaRegistryClientConfig extends io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig {

    /**
     * A configuration to enable/disable validating schema field names when parsing Avro schemas.
     */
    public static final String SCHEMA_REGISTRY_PARSER_VALIDATE_FIELD_NAMES = "schema.registry.parser.validate.names";

    /**
     * A configuration to enable/disable validating schema field defaults when parsing Avro schemas.
     */
    public static final String SCHEMA_REGISTRY_PARSER_VALIDATE_DEFAULTS = "schema.registry.parser.validate.defaults";

}
