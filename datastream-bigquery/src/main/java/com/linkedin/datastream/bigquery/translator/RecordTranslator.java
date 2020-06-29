/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.bigquery.translator;

import java.math.BigDecimal;
import java.math.BigInteger;

import java.nio.ByteBuffer;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import com.google.cloud.bigquery.InsertAllRequest;

/**
 * This class translates given avro record into BQ row object.
 */
public class RecordTranslator {

    private static boolean isPrimitiveType(Schema.Type type) {
        return (type == Schema.Type.BOOLEAN ||
                type == Schema.Type.INT ||
                type == Schema.Type.LONG ||
                type == Schema.Type.FLOAT ||
                type == Schema.Type.DOUBLE ||
                type == Schema.Type.BYTES ||
                type == Schema.Type.STRING);
    }

    private static Map.Entry<String, Object> translatePrimitiveTypeObject(Object record, Schema avroSchema, String name) {
        Map.Entry<String, Object> result = new AbstractMap.SimpleEntry<>(name, null);
        switch (avroSchema.getType()) {
            case STRING:
                if (LogicalTypeIdentifier.isTimeType(avroSchema)) {
                    result = new AbstractMap.SimpleEntry<>(name, LogicalTypeTranslator.translateTimeType(String.valueOf(record), avroSchema));
                    break;
                } else if (LogicalTypeIdentifier.isTimestampType(avroSchema)) {
                    result = new AbstractMap.SimpleEntry<>(name, LogicalTypeTranslator.translateTimestampType(String.valueOf(record), avroSchema));
                    break;
                } else {
                    result = new AbstractMap.SimpleEntry<>(name, String.valueOf(record));
                    break;
                }
            case FLOAT:
            case DOUBLE:
                result = new AbstractMap.SimpleEntry<>(name, record);
                break;
            case INT:
                if (LogicalTypeIdentifier.isDateType(avroSchema)) {
                    result = new AbstractMap.SimpleEntry<>(name, LogicalTypeTranslator.translateDateType((Long) record));
                    break;
                }
            case LONG:
                if (LogicalTypeIdentifier.isTimeType(avroSchema)) {
                    result = new AbstractMap.SimpleEntry<>(name, LogicalTypeTranslator.translateTimeType((Long) record, avroSchema));
                    break;
                } else if (LogicalTypeIdentifier.isTimestampType(avroSchema)) {
                    result = new AbstractMap.SimpleEntry<>(name, LogicalTypeTranslator.translateTimestampType((Long) record, avroSchema));
                    break;
                } else {
                    result = new AbstractMap.SimpleEntry<>(name, record);
                    break;
                }
            case BOOLEAN:
                result = new AbstractMap.SimpleEntry<>(name, record);
                break;
            case BYTES:
                if (LogicalTypeIdentifier.isDecimalType(avroSchema)) {
                    final LogicalTypes.Decimal decimalType = (LogicalTypes.Decimal) avroSchema.getLogicalType();
                    result = new AbstractMap.SimpleEntry<>(name, new BigDecimal(new BigInteger(((ByteBuffer) record).array()), decimalType.getScale()));
                } else {
                    result = new AbstractMap.SimpleEntry<>(name, ((ByteBuffer) record).array());
                }
                break;
            default:
                return result;
        }
        return result;
    }

    private static Map.Entry<String, Object> translateEnumTypeObject(Object record, String name) {
        return new AbstractMap.SimpleEntry<>(name, record);
    }

    private static Map.Entry<String, Object> translateFixedTypeObject(Object record, Schema avroSchema, String name) {
        Map.Entry<String, Object> result;
        if (LogicalTypeIdentifier.isDecimalType(avroSchema)) {
            final LogicalTypes.Decimal decimalType = (LogicalTypes.Decimal) avroSchema.getLogicalType();
            result = new AbstractMap.SimpleEntry<>(name, new BigDecimal(new BigInteger(((GenericFixed) record).bytes()), decimalType.getScale()));
        } else {
            result = new AbstractMap.SimpleEntry<>(name, ((GenericFixed) record).bytes());
        }
        return result;
    }

    private static Map.Entry<String, Object> translateUnionTypeObject(Object record, Schema avroSchema, String name) {

        Map.Entry<String, Object> result = new AbstractMap.SimpleEntry<>(name, null);

        if (record == null) {
            return result;
        }

        if (avroSchema.getTypes().size() == 2 &&
                (avroSchema.getTypes().get(0).getType() == Schema.Type.NULL ||
                        avroSchema.getTypes().get(1).getType() == Schema.Type.NULL)
        ) {
            Schema typeSchema = (avroSchema.getTypes().get(0).getType() != Schema.Type.NULL) ?
                    avroSchema.getTypes().get(0) : avroSchema.getTypes().get(1);

            if (isPrimitiveType(typeSchema.getType())) {
                result = translatePrimitiveTypeObject(record, typeSchema, name);
            } else if (typeSchema.getType() == Schema.Type.RECORD) {
                result = new AbstractMap.SimpleEntry<>(name, translateRecord((GenericRecord) record, typeSchema));
            } else if (typeSchema.getType() == Schema.Type.UNION) {
                result = translateUnionTypeObject(record, typeSchema, name);
            } else if (typeSchema.getType() == Schema.Type.FIXED) {
                result = new AbstractMap.SimpleEntry<>(name, translateFixedTypeObject(record, typeSchema, typeSchema.getName()));
            } else if (typeSchema.getType() == Schema.Type.MAP) {
                result = translateMapTypeObject(record, typeSchema, name);
            } else if (typeSchema.getType() == Schema.Type.ENUM) {
                result = new AbstractMap.SimpleEntry<>(name, translateEnumTypeObject(record, typeSchema.getName()));
            } else if (typeSchema.getType() == Schema.Type.ARRAY) {
                result = translateArrayTypeObject(record, typeSchema, name);
            }

        } else {
            if (record instanceof Boolean) {
                result = new AbstractMap.SimpleEntry<>(Schema.Type.BOOLEAN.name().toLowerCase() + "_value", record);
            } else if (record instanceof Integer) {
                result = new AbstractMap.SimpleEntry<>(Schema.Type.INT.name().toLowerCase() + "_value", record);
            } else if (record instanceof Long) {
                result = new AbstractMap.SimpleEntry<>(Schema.Type.LONG.name().toLowerCase() + "_value", record);
            } else if (record instanceof Float) {
                result = new AbstractMap.SimpleEntry<>(Schema.Type.FLOAT.name().toLowerCase() + "_value", record);
            } else if (record instanceof Double) {
                result = new AbstractMap.SimpleEntry<>(Schema.Type.DOUBLE.name().toLowerCase() + "_value", record);
            } else if (record instanceof ByteBuffer) {
                result = new AbstractMap.SimpleEntry<>(Schema.Type.BYTES.name().toLowerCase() + "_value", record);
            } else if (record instanceof String || record instanceof Utf8) {
                result = new AbstractMap.SimpleEntry<>(Schema.Type.STRING.name().toLowerCase() + "_value", String.valueOf(record));
            }
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private static Map.Entry<String, Object> translateMapTypeObject(Object record, Schema avroSchema, String name) {
        Map.Entry<String, Object> result = new AbstractMap.SimpleEntry<>(name, null);
        Map<String, Object> subRecords = new HashMap<>();

        if (isPrimitiveType(avroSchema.getValueType().getType())) {
            Map<String, Object> map = (Map<String, Object>) record;
            subRecords = new HashMap<>();
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                subRecords.put("key", entry.getKey());
                subRecords.put("value", entry.getValue());
            }
            result = new AbstractMap.SimpleEntry<>(name, subRecords);
        } else if (avroSchema.getValueType().getType() == Schema.Type.RECORD) {
            Map<String, Object> map = (Map<String, Object>) record;
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                subRecords.put("key", entry.getKey());
                subRecords.put("value", translateRecord((GenericRecord) entry.getValue(), avroSchema.getValueType()));
            }
            result = new AbstractMap.SimpleEntry<>(name, subRecords);
        } else if (avroSchema.getValueType().getType() == Schema.Type.UNION) {
            Map<String, Object> map = (Map<String, Object>) record;
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                subRecords.put("key", entry.getKey());
                subRecords.put("value", translateUnionTypeObject(entry.getValue(), avroSchema.getValueType(), avroSchema.getValueType().getName()));
            }
            result = new AbstractMap.SimpleEntry<>(name, subRecords);
        } else if (avroSchema.getValueType().getType() == Schema.Type.FIXED) {
            Map<String, Object> map = (Map<String, Object>) record;
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                subRecords.put("key", entry.getKey());
                subRecords.put("value", translateFixedTypeObject(entry.getValue(), avroSchema.getValueType(), avroSchema.getValueType().getName()));
            }
            result = new AbstractMap.SimpleEntry<>(name, subRecords);
        } else if (avroSchema.getValueType().getType() == Schema.Type.MAP) {
            Map<String, Object> map = (Map<String, Object>) record;
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                subRecords.put("key", entry.getKey());
                subRecords.put("value", translateMapTypeObject(entry.getValue(), avroSchema.getValueType(), avroSchema.getValueType().getName()));
            }
            result = new AbstractMap.SimpleEntry<>(name, subRecords);
        } else if (avroSchema.getValueType().getType() == Schema.Type.ENUM) {
            Map<String, Object> map = (Map<String, Object>) record;
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                subRecords.put("key", entry.getKey());
                subRecords.put("value", translateEnumTypeObject(entry.getValue(), avroSchema.getValueType().getName()));
            }
            result = new AbstractMap.SimpleEntry<>(name, subRecords);
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private static Map.Entry<String, Object> translateArrayTypeObject(Object record, Schema avroSchema, String name) {
        Map.Entry<String, Object> result;
        if (avroSchema.getElementType().getType() == Schema.Type.ARRAY) {
            throw new IllegalArgumentException("Array of array types are not supported.");
        }
        if (avroSchema.getElementType().getType() == Schema.Type.RECORD) {
            List<Map<String, Object>> sRecords = new ArrayList<>();
            for (GenericRecord rec : (GenericArray<GenericRecord>) record) {
                sRecords.add(translateRecord(rec, avroSchema.getElementType()));
            }
            result = new AbstractMap.SimpleEntry<>(name, sRecords);
        } else {
            result = new AbstractMap.SimpleEntry<>(name, record);
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> translateRecord(GenericRecord avroRecord, Schema avroSchema) {

        if (avroSchema.getType() != Schema.Type.RECORD) {
            throw new IllegalArgumentException("Object is not a Avro Record type.");
        }

        Map<String, Object> fields = new HashMap<>();
        for (org.apache.avro.Schema.Field avroField: avroSchema.getFields()) {
            if (isPrimitiveType(avroField.schema().getType())) {
                Map.Entry<String, Object> entry = translatePrimitiveTypeObject(avroRecord.get(avroField.name()), avroField.schema(), avroField.name());
                fields.put(entry.getKey(), entry.getValue());
            } else if (avroField.schema().getType() == Schema.Type.RECORD) {
                if (avroRecord.get(avroField.name()) != null) {
                    fields.put(avroField.name(), translateRecord((GenericRecord) avroRecord.get(avroField.name()), avroField.schema()));
                }
            } else if (avroField.schema().getType() == Schema.Type.UNION) {
                Map.Entry<String, Object> entry = translateUnionTypeObject(avroRecord.get(avroField.name()), avroField.schema(), avroField.name());
                fields.put(entry.getKey(), entry.getValue());
            } else if (avroField.schema().getType() == Schema.Type.FIXED) {
                Map.Entry<String, Object> entry = translateFixedTypeObject(avroRecord.get(avroField.name()), avroField.schema(), avroField.name());
                fields.put(entry.getKey(), entry.getValue());
            } else if (avroField.schema().getType() == Schema.Type.MAP) {
                Map.Entry<String, Object> entry = translateMapTypeObject(avroRecord.get(avroField.name()), avroField.schema(), avroField.name());
                fields.put(entry.getKey(), entry.getValue());
            } else if (avroField.schema().getType() == Schema.Type.ENUM) {
                Map.Entry<String, Object> entry = translateEnumTypeObject(avroRecord.get(avroField.name()), avroField.name());
                fields.put(entry.getKey(), entry.getValue());
            } else if (avroField.schema().getType() == Schema.Type.ARRAY) {
                Map.Entry<String, Object> entry = translateArrayTypeObject(avroRecord.get(avroField.name()), avroField.schema(), avroField.name());
                fields.put(entry.getKey(), entry.getValue());
            }
        }
        return fields;
    }

    /**
     * translate given avro record into BQ row object.
     * @param avroRecord avro record
     * @param avroSchema avro schema
     * @return BQ row
     */
    public static InsertAllRequest.RowToInsert translate(GenericRecord avroRecord, Schema avroSchema) {
        if (avroSchema.getType() != org.apache.avro.Schema.Type.RECORD) {
            throw new IllegalArgumentException("The root of the record's schema should be a RECORD type.");
        }
        return InsertAllRequest.RowToInsert.of(translateRecord(avroRecord, avroSchema));
    }

}


