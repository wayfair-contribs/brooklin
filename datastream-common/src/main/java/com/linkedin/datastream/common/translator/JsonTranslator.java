/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common.translator;

import java.nio.charset.StandardCharsets;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import tech.allegro.schema.json2avro.converter.JsonAvroConverter;

/**
 * Connector that implements RecordTranslator and SchemaTranslator to support Json.
 */
public class JsonTranslator implements RecordTranslator<String, GenericRecord>, SchemaTranslator<Schema, String> {

    public static final String SCHEMA_FIELD_NAME = "schema";
    public static final String PAYLOAD_FIELD_NAME = "payload";
    /**
     * Translates values from internal format into Json String
     *
     * @param sourceRecord - Source schema to be translated
     * @return The translated record in T format
     */
    @Override
    public String translateSchema(Schema sourceRecord) throws Exception {
        return sourceRecord.toString();
    }

    private static final String CLASS_NAME = JsonTranslator.class.getSimpleName();
    private static final Logger LOG = LoggerFactory.getLogger(CLASS_NAME);

    /**
     * Translates values from internal format into Json String.
     *
     * @param record - The record to be translated into the internal format
     * @param includeSchema - Flag to include schema
     * @return The translated record in T format
     * @throws Exception if any error occurs during creation
     */
    @Override
    public String translateFromInternalFormat(GenericRecord record, boolean includeSchema) throws Exception {
        JsonAvroConverter converter = new JsonAvroConverter();
        if (includeSchema) {
            ObjectMapper objectMapper = new ObjectMapper();
            ObjectNode withSchemaJson = JsonNodeFactory.instance.objectNode();
            withSchemaJson.set(SCHEMA_FIELD_NAME, objectMapper.readTree(translateSchema(record.getSchema())));
            withSchemaJson.set(PAYLOAD_FIELD_NAME, objectMapper.readTree(converter.convertToJson(record)));
            return withSchemaJson.toString();
        } else {
            return new String(converter.convertToJson(record), StandardCharsets.UTF_8);
        }

    }

}
