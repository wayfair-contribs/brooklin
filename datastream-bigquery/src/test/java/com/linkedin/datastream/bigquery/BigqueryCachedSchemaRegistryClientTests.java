/*
 * Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */

package com.linkedin.datastream.bigquery;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.testng.annotations.Test;
import org.testng.reporters.Files;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class BigqueryCachedSchemaRegistryClientTests {

    @Test
    public void testParseSchemasWithoutValidation() throws IOException, RestClientException {
        final RestService restService = mock(RestService.class);
        final BigqueryCachedSchemaRegistryClient srClient = new BigqueryCachedSchemaRegistryClient(restService, 2);
        final Schema.Parser parser = new Schema.Parser().setValidate(false).setValidateDefaults(false);

        final int invalidDefaultsSchemaId = 1;
        String invalidDefaultsSchemaString;
        try (final InputStream avroFileInputStream = getClass().getClassLoader().getResourceAsStream("invalid_defaults_schema.avsc")) {
            invalidDefaultsSchemaString = Files.readFile(avroFileInputStream);
        }
        when(restService.getId(invalidDefaultsSchemaId)).thenReturn(new SchemaString(invalidDefaultsSchemaString));

        final int invalidFieldNamesSchemaId = 2;
        String invalidFieldNamesSchemaString;
        try (final InputStream avroFileInputStream = getClass().getClassLoader().getResourceAsStream("invalid_field_name_schema.avsc")) {
            invalidFieldNamesSchemaString = Files.readFile(avroFileInputStream);
        }
        when(restService.getId(invalidFieldNamesSchemaId)).thenReturn(new SchemaString(invalidFieldNamesSchemaString));

        assertEquals(srClient.getById(invalidDefaultsSchemaId), parser.parse(invalidDefaultsSchemaString));
        assertEquals(srClient.getByID(invalidFieldNamesSchemaId), parser.parse(invalidFieldNamesSchemaString));
    }

    @Test
    public void testParseSchemaWithInvalidDefaultsValidationDisabled() throws IOException, RestClientException {
        final RestService restService = mock(RestService.class);
        final BigqueryCachedSchemaRegistryClient srClient = new BigqueryCachedSchemaRegistryClient(restService, 1);
        final int schemaId = 12345;
        String schemaString;
        try (final InputStream avroFileInputStream = getClass().getClassLoader().getResourceAsStream("invalid_defaults_schema.avsc")) {
            schemaString = Files.readFile(avroFileInputStream);
        }
        final Schema schema = new Schema.Parser().setValidateDefaults(false).parse(schemaString);
        when(restService.getId(schemaId)).thenReturn(new SchemaString(schemaString));
        final Schema fetchedSchema = srClient.getById(schemaId);
        assertEquals(fetchedSchema, schema);
    }

    @Test(expectedExceptions = AvroTypeException.class, expectedExceptionsMessageRegExp = "Invalid default for field .+")
    public void testParseSchemaWithInvalidDefaultsValidationEnabled() throws IOException, RestClientException {
        final RestService restService = mock(RestService.class);
        final Map<String, ?> configs = ImmutableMap.of(
                BigquerySchemaRegistryClientConfig.SCHEMA_REGISTRY_PARSER_VALIDATE_DEFAULTS, true
        );
        final BigqueryCachedSchemaRegistryClient srClient = new BigqueryCachedSchemaRegistryClient(restService, 1, configs);
        final int schemaId = 12345;
        String schemaString;
        try (final InputStream avroFileInputStream = getClass().getClassLoader().getResourceAsStream("invalid_defaults_schema.avsc")) {
            schemaString = Files.readFile(avroFileInputStream);
        }
        when(restService.getId(schemaId)).thenReturn(new SchemaString(schemaString));
        srClient.getById(schemaId);
    }

    @Test
    public void testParseSchemaWithInvalidFieldNamesValidationDisabled() throws IOException, RestClientException {
        final RestService restService = mock(RestService.class);
        final BigqueryCachedSchemaRegistryClient srClient = new BigqueryCachedSchemaRegistryClient(restService, 1);
        final int schemaId = 12345;
        String schemaString;
        try (final InputStream avroFileInputStream = getClass().getClassLoader().getResourceAsStream("invalid_field_name_schema.avsc")) {
            schemaString = Files.readFile(avroFileInputStream);
        }
        final Schema schema = new Schema.Parser().setValidate(false).parse(schemaString);
        when(restService.getId(schemaId)).thenReturn(new SchemaString(schemaString));
        final Schema fetchedSchema = srClient.getById(schemaId);
        assertEquals(fetchedSchema, schema);
    }

    @Test(expectedExceptions = SchemaParseException.class, expectedExceptionsMessageRegExp = "Illegal initial character: .+")
    public void testParseSchemaWithInvalidFieldNamesValidationEnabled() throws IOException, RestClientException {
        final RestService restService = mock(RestService.class);
        final Map<String, ?> configs = ImmutableMap.of(
                BigquerySchemaRegistryClientConfig.SCHEMA_REGISTRY_PARSER_VALIDATE_FIELD_NAMES, true
        );
        final BigqueryCachedSchemaRegistryClient srClient = new BigqueryCachedSchemaRegistryClient(restService, 1, configs);
        final int schemaId = 12345;
        String schemaString;
        try (final InputStream avroFileInputStream = getClass().getClassLoader().getResourceAsStream("invalid_field_name_schema.avsc")) {
            schemaString = Files.readFile(avroFileInputStream);
        }
        when(restService.getId(schemaId)).thenReturn(new SchemaString(schemaString));
        srClient.getById(schemaId);
    }

}
