/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.bigquery.translator;

import java.util.ArrayList;
import java.util.List;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;

/**
 * This class translates given avro schema into BQ schema.
 */
public class SchemaTranslator {

    private static class FieldTypePair {
        Field field;
        StandardSQLTypeName type;

        FieldTypePair(Field field, StandardSQLTypeName type) {
            this.field = field;
            this.type = type;
        }
    }

    private static FieldTypePair translateSchema(org.apache.avro.Schema avroSchema, String name) {

        Field.Builder fieldBuilder;
        StandardSQLTypeName type;
        FieldTypePair fieldTypePair;

        List<Field> fieldList = new ArrayList<>();

        switch (avroSchema.getType()) {
            case RECORD:
                for (org.apache.avro.Schema.Field avroField: avroSchema.getFields()) {
                    fieldList.add(translateSchema(avroField.schema(), avroField.name()).field);
                }
                type = StandardSQLTypeName.STRUCT;
                fieldBuilder = Field.newBuilder(avroSchema.getName(), type, FieldList.of(fieldList));
                break;
            case ENUM:
                type = StandardSQLTypeName.STRING;
                fieldBuilder = Field.newBuilder(avroSchema.getName(), type);
                break;
            case ARRAY:
                if (avroSchema.getElementType().getType() == org.apache.avro.Schema.Type.ARRAY) {
                    throw new IllegalArgumentException("Array of array types are not supported.");
                }
                fieldTypePair = translateSchema(avroSchema.getElementType(), name);
                type = fieldTypePair.type;

                if (fieldTypePair.type == StandardSQLTypeName.STRUCT) {
                    fieldBuilder = Field.newBuilder(name, type, fieldTypePair.field);
                } else {
                    fieldBuilder = Field.newBuilder(name, type);
                }
                fieldBuilder.setMode(Field.Mode.REPEATED);
                break;
            case MAP:
                type = StandardSQLTypeName.STRUCT;
                fieldList = FieldList.of(
                        Field.newBuilder("key", StandardSQLTypeName.STRING).build(),
                        Field.newBuilder("value", translateSchema(avroSchema.getValueType(), name).type).build()
                );
                fieldBuilder = Field.newBuilder(name, type, FieldList.of(fieldList));
                break;
            case UNION:
                if (avroSchema.getTypes().size() == 2 &&
                        (avroSchema.getTypes().get(0).getType() == org.apache.avro.Schema.Type.NULL ||
                                avroSchema.getTypes().get(1).getType() == org.apache.avro.Schema.Type.NULL)
                ) {
                    fieldTypePair = translateSchema(avroSchema.getTypes().get(0), name);
                    type = fieldTypePair.type;
                    fieldBuilder = Field.newBuilder(name, type).setMode(Field.Mode.NULLABLE);
                } else {
                    for (org.apache.avro.Schema uType: avroSchema.getTypes()) {
                        if (uType.getType() == org.apache.avro.Schema.Type.NULL) {
                            continue;
                        }
                        Field.Builder fb = Field.newBuilder(uType.getType().name().toLowerCase() + "_value", translateSchema(uType, name).type);
                        fb.setMode(Field.Mode.NULLABLE);
                        fieldList.add(fb.build());
                    }
                    type = StandardSQLTypeName.STRUCT;
                    fieldBuilder = Field.newBuilder(name, type, FieldList.of(fieldList));
                }
                break;
            case FIXED:
                if (avroSchema.getLogicalType() != null && avroSchema.getLogicalType().getName().toLowerCase().equals("decimal")) {
                    type = StandardSQLTypeName.NUMERIC;
                    fieldBuilder = Field.newBuilder(name, type);
                    break;
                }
            case BYTES:
                // covers logical type Duration as well
                type = StandardSQLTypeName.BYTES;
                fieldBuilder = Field.newBuilder(name, type);
                break;
            case BOOLEAN:
                type = StandardSQLTypeName.BOOL;
                fieldBuilder = Field.newBuilder(name, type);
                break;
            case INT:
                if (avroSchema.getLogicalType() != null && avroSchema.getLogicalType().getName().toLowerCase().equals("date")) {
                    type = StandardSQLTypeName.DATE;
                    fieldBuilder = Field.newBuilder(name, type);
                    break;
                }
            case LONG:
                if (avroSchema.getLogicalType() != null && avroSchema.getLogicalType().getName().toLowerCase().equals("time")) {
                    type = StandardSQLTypeName.TIME;
                    fieldBuilder = Field.newBuilder(name, type);
                    break;
                } else if (avroSchema.getLogicalType() != null && avroSchema.getLogicalType().getName().toLowerCase().equals("timestamp")) {
                    type = StandardSQLTypeName.TIMESTAMP;
                    fieldBuilder = Field.newBuilder(name, type);
                    break;
                } else {
                    type = StandardSQLTypeName.INT64;
                    fieldBuilder = Field.newBuilder(name, type);
                    break;
                }
            case FLOAT:
            case DOUBLE:
                type = StandardSQLTypeName.FLOAT64;
                fieldBuilder = Field.newBuilder(name, type);
                break;
            case STRING:
                type = StandardSQLTypeName.STRING;
                fieldBuilder = Field.newBuilder(name, type);
                break;
            default:
                throw new IllegalArgumentException("Avro type not recognized.");
        }

        if (avroSchema.getDoc() != null) {
            fieldBuilder.setDescription(avroSchema.getDoc());
        }
        return new FieldTypePair(fieldBuilder.build(), type);
    }

    /**
     * Translate given avro schema into BQ schema
     * @param avroSchema avro schema
     * @return BQ schema
     */
    public static Schema translate(org.apache.avro.Schema avroSchema) {

        if (avroSchema.getType() != org.apache.avro.Schema.Type.RECORD) {
            throw new IllegalArgumentException("The root of the record's schema should be a RECORD type.");
        }

        return Schema.of(translateSchema(avroSchema, avroSchema.getName()).field);
    }
}

