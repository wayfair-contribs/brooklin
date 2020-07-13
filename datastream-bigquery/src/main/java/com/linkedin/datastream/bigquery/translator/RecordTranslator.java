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
import java.util.List;
import java.util.Map;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
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
                    result = new AbstractMap.SimpleEntry<>(name, LogicalTypeTranslator.translateDateType((Integer) record));
                    break;
                } else if (LogicalTypeIdentifier.isTimeType(avroSchema)) {
                    result = new AbstractMap.SimpleEntry<>(name, LogicalTypeTranslator.translateTimeType((Integer) record, avroSchema));
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
                // TODO: map of bytes
                /*if (LogicalTypeIdentifier.isDecimalType(avroSchema)) {
                    final LogicalTypes.Decimal decimalType = (LogicalTypes.Decimal) avroSchema.getLogicalType();
                    result = new AbstractMap.SimpleEntry<>(name, new BigDecimal(new BigInteger(((ByteBuffer) record).array()), decimalType.getScale()));
                } else {
                    result = new AbstractMap.SimpleEntry<>(name, ((ByteBuffer) record).array());
                }*/
                break;
            default:
                return result;
        }
        return result;
    }

    private static Map.Entry<String, Object> translateEnumTypeObject(Object record, String name) {
        return new AbstractMap.SimpleEntry<>(name, String.valueOf(record));
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
                result = new AbstractMap.SimpleEntry<>(name, translateRecord((GenericData.Record) record));
            } else if (typeSchema.getType() == Schema.Type.UNION) {
                result = translateUnionTypeObject(record, typeSchema, name);
            } else if (typeSchema.getType() == Schema.Type.FIXED) {
                result = new AbstractMap.SimpleEntry<>(name, translateFixedTypeObject(record, typeSchema, typeSchema.getName()));
            } else if (typeSchema.getType() == Schema.Type.MAP) {
                result = translateMapTypeObject(record, typeSchema, name);
            } else if (typeSchema.getType() == Schema.Type.ENUM) {
                result = new AbstractMap.SimpleEntry<>(name, String.valueOf(translateEnumTypeObject(record, "").getValue()));
            } else if (typeSchema.getType() == Schema.Type.ARRAY) {
                result = translateArrayTypeObject(record, typeSchema, name);
            }

        } else {
            TableRow fieldVaules = new TableRow();
            if (record instanceof Boolean) {
                fieldVaules.put(Schema.Type.BOOLEAN.name().toLowerCase() + "_value", record);
            } else if (record instanceof Integer) {
                LogicalType logicalType = null;
                Schema subTypeSchema = null;
                for (Schema schema: avroSchema.getTypes()) {
                    if (schema.getType() == Schema.Type.INT) {
                        subTypeSchema = schema;
                        logicalType = schema.getLogicalType();
                        break;
                    }
                }

                if (logicalType != null) {
                    if (LogicalTypeIdentifier.isDateType(subTypeSchema)) {
                        fieldVaules.put(Schema.Type.INT.name().toLowerCase() + "_date_value", LogicalTypeTranslator.translateDateType((Long) record));
                    } else if (LogicalTypeIdentifier.isMilliTime(subTypeSchema)) {
                        fieldVaules.put(Schema.Type.INT.name().toLowerCase() + "_time_millis_value", LogicalTypeTranslator.translateTimeType((Long) record, subTypeSchema));
                    }
                } else {
                    fieldVaules.put(Schema.Type.INT.name().toLowerCase() + "_value", record);
                }
            } else if (record instanceof Long) {
                LogicalType logicalType = null;
                Schema subTypeSchema = null;
                for (Schema schema: avroSchema.getTypes()) {
                    if (schema.getType() == Schema.Type.LONG) {
                        subTypeSchema = schema;
                        logicalType = schema.getLogicalType();
                        break;
                    }
                }

                if (logicalType != null) {
                    if (LogicalTypeIdentifier.isMicroTime(subTypeSchema)) {
                        fieldVaules.put(Schema.Type.LONG.name().toLowerCase() + "_time_micro_value", LogicalTypeTranslator.translateTimeType((Long) record, subTypeSchema));
                    } else if (LogicalTypeIdentifier.isTimestampType(subTypeSchema)) {
                        fieldVaules.put(Schema.Type.LONG.name().toLowerCase() + "_" + logicalType.getName().replace("-", "_") + "_value", LogicalTypeTranslator.translateTimestampType((Long) record, subTypeSchema));
                    }
                } else {
                    fieldVaules.put(Schema.Type.LONG.name().toLowerCase() + "_value", record);
                }
            } else if (record instanceof Float) {
                fieldVaules.put(Schema.Type.FLOAT.name().toLowerCase() + "_value", record);
            } else if (record instanceof Double) {
                fieldVaules.put(Schema.Type.DOUBLE.name().toLowerCase() + "_value", record);
            } else if (record instanceof ByteBuffer) {
                LogicalType logicalType = null;
                for (Schema schema: avroSchema.getTypes()) {
                    if (schema.getType() == Schema.Type.BYTES) {
                        logicalType = schema.getLogicalType();
                        break;
                    }
                }

                if (logicalType instanceof LogicalTypes.Decimal) {
                    if (LogicalTypeIdentifier.isDecimalType(avroSchema)) {
                        fieldVaules.put(Schema.Type.BYTES.name().toLowerCase() + "_decimal_value", new BigDecimal(new BigInteger(((ByteBuffer) record).array()), ((LogicalTypes.Decimal) logicalType).getScale()));
                    }
                } else {
                    fieldVaules.put(Schema.Type.BYTES.name().toLowerCase() + "_value", record);
                }
            } else if (record instanceof GenericFixed) {
                LogicalType logicalType = null;
                Schema subTypeSchema = null;
                for (Schema schema: avroSchema.getTypes()) {
                    if (schema.getType() == Schema.Type.FIXED) {
                        subTypeSchema = schema;
                        logicalType = schema.getLogicalType();
                        break;
                    }
                }

                if (logicalType instanceof LogicalTypes.Decimal) {
                    fieldVaules.put(subTypeSchema.getName() + "_decimal_value", new BigDecimal(new BigInteger(((ByteBuffer) record).array()), ((LogicalTypes.Decimal) logicalType).getScale()));
                } else {
                    fieldVaules.put(subTypeSchema.getName() + "_value", record);
                }
            } else if (record instanceof String || record instanceof Utf8) {
                // do not know how to handle enums
                fieldVaules.put(Schema.Type.STRING.name().toLowerCase() + "_value", String.valueOf(record));
            } else if (record instanceof GenericArray) {
                LogicalType logicalType = null;
                Schema subTypeSchema = null;
                for (Schema schema: avroSchema.getTypes()) {
                    if (schema.getType() == Schema.Type.ARRAY) {
                        subTypeSchema = schema;
                        logicalType = schema.getElementType().getLogicalType();
                        break;
                    }
                }

                Map.Entry<String, Object> subRec = translateArrayTypeObject(record, subTypeSchema, name);
                if (logicalType != null) {
                    fieldVaules.put("array_" + subTypeSchema.getElementType().getName() + "_" + logicalType.getName().replace("-", "_") + "_value", subRec.getValue());
                } else {
                    fieldVaules.put("array_" + subTypeSchema.getElementType().getName() + "_value", subRec.getValue());
                }

            } else if (record instanceof Map) {
                LogicalType logicalType = null;
                Schema subTypeSchema = null;
                for (Schema schema: avroSchema.getTypes()) {
                    if (schema.getType() == Schema.Type.MAP) {
                        subTypeSchema = schema;
                        logicalType = schema.getValueType().getLogicalType();
                        break;
                    }
                }

                Map.Entry<String, Object> subRec = translateMapTypeObject(record, subTypeSchema, name);
                if (logicalType != null) {
                    fieldVaules.put("map_" + subTypeSchema.getValueType().getName() + "_" + logicalType.getName().replace("-", "_") + "_value", subRec.getValue());
                } else {
                    fieldVaules.put("map_" + subTypeSchema.getValueType().getName() + "_value", subRec.getValue());
                }

            } else if (record instanceof GenericRecord) {
                GenericData.Record rec = (GenericData.Record) record;
                fieldVaules.put(rec.getSchema().getName() + "_value", translateRecord(rec));
            } else if (record instanceof GenericData.EnumSymbol) {
                GenericData.EnumSymbol rec = (GenericData.EnumSymbol) record;
                fieldVaules.put(rec.getSchema().getName() + "_value", String.valueOf(rec));
            }

            result = new AbstractMap.SimpleEntry<>(name, fieldVaules);
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private static Map.Entry<String, Object> translateMapTypeObject(Object record, Schema avroSchema, String name) {
        Map.Entry<String, Object> result = new AbstractMap.SimpleEntry<>(name, null);
        List<Map<String, Object>> subRecords = new ArrayList<>();

        if (isPrimitiveType(avroSchema.getValueType().getType())) {
            //TODO: ignore bytes type
            if (avroSchema.getValueType().getType() == Schema.Type.BYTES) {
                return result;
            }

            Map<String, Object> map = (Map<String, Object>) record;
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                TableRow subRecord = new TableRow();
                subRecord.put("key", String.valueOf(entry.getKey()));
                subRecord.put("value", translatePrimitiveTypeObject(entry.getValue(), avroSchema.getValueType(), "").getValue());
                subRecords.add(subRecord);
            }
            result = new AbstractMap.SimpleEntry<>(name, subRecords);
        } else if (avroSchema.getValueType().getType() == Schema.Type.RECORD) {
            Map<String, Object> map = (Map<String, Object>) record;
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                TableRow subRecord = new TableRow();
                subRecord.put("key", String.valueOf(entry.getKey()));
                subRecord.put("value", translateRecord((GenericData.Record) entry.getValue()));
                subRecords.add(subRecord);
            }
            result = new AbstractMap.SimpleEntry<>(name, subRecords);
        } else if (avroSchema.getValueType().getType() == Schema.Type.UNION) {
            Map<String, Object> map = (Map<String, Object>) record;
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                TableRow subRecord = new TableRow();
                subRecord.put("key", String.valueOf(entry.getKey()));
                subRecord.put("value", translateUnionTypeObject(entry.getValue(), avroSchema.getValueType(), avroSchema.getValueType().getName()));
                subRecords.add(subRecord);
            }
            result = new AbstractMap.SimpleEntry<>(name, subRecords);
        } else if (avroSchema.getValueType().getType() == Schema.Type.FIXED) {
            Map<String, Object> map = (Map<String, Object>) record;
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                TableRow subRecord = new TableRow();
                subRecord.put("key", String.valueOf(entry.getKey()));
                subRecord.put("value", translateFixedTypeObject(entry.getValue(), avroSchema.getValueType(), avroSchema.getValueType().getName()));
                subRecords.add(subRecord);
            }
            result = new AbstractMap.SimpleEntry<>(name, subRecords);
        } else if (avroSchema.getValueType().getType() == Schema.Type.MAP) {
            Map<String, Object> map = (Map<String, Object>) record;
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                TableRow subRecord = new TableRow();
                subRecord.put("key", String.valueOf(entry.getKey()));
                subRecord.put("value", translateMapTypeObject(entry.getValue(), avroSchema.getValueType(), avroSchema.getValueType().getName()).getValue());
                subRecords.add(subRecord);
            }
            result = new AbstractMap.SimpleEntry<>(name, subRecords);
        } else if (avroSchema.getValueType().getType() == Schema.Type.ENUM) {
            Map<String, Object> map = (Map<String, Object>) record;
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                TableRow subRecord = new TableRow();
                subRecord.put("key", String.valueOf(entry.getKey()));
                subRecord.put("value", String.valueOf(translateEnumTypeObject(entry.getValue(), avroSchema.getValueType().getName()).getValue()));
                subRecords.add(subRecord);
            }
            result = new AbstractMap.SimpleEntry<>(name, subRecords);
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private static Map.Entry<String, Object> translateArrayTypeObject(Object record, Schema avroSchema, String name) {
        Map.Entry<String, Object> result;
        //TODO: ignore bytes type
        if (avroSchema.getElementType().getType() == Schema.Type.BYTES) {
            return new AbstractMap.SimpleEntry<>(name, null);
        }

        if (avroSchema.getElementType().getType() == Schema.Type.ARRAY) {
            throw new IllegalArgumentException("Array of array types are not supported.");
        }
        if (avroSchema.getElementType().getType() == Schema.Type.RECORD) {
            List<TableRow> sRecords = new ArrayList<>();
            for (GenericRecord rec : (GenericArray<GenericRecord>) record) {
                sRecords.add(translateRecord((GenericData.Record) rec));
            }
            result = new AbstractMap.SimpleEntry<>(name, sRecords);
        } else if (avroSchema.getElementType().getType() == Schema.Type.MAP) {
            List<Map<String, Object>> sRecords = new ArrayList<>();
            for (Object rec : (GenericArray<Object>) record) {
                Map.Entry<String, Object> subMap = translateMapTypeObject(rec, avroSchema.getElementType(), "");
                sRecords.addAll((List<TableRow>) subMap.getValue());
            }
            result = new AbstractMap.SimpleEntry<>(name, sRecords);
        } else if (avroSchema.getElementType().getType() == Schema.Type.UNION) {
            List<Object> sRecords = new ArrayList<>();
            for (Object rec : (GenericArray<Object>) record) {
                Map.Entry<String, Object> subMap = translateUnionTypeObject(rec, avroSchema.getElementType(), "");
                sRecords.add(subMap.getValue());
            }
            result = new AbstractMap.SimpleEntry<>(name, sRecords);
        } else if (avroSchema.getElementType().getType() == Schema.Type.ENUM) {
            List<Object> sRecords = new ArrayList<>();
            for (Object rec : (GenericArray<Object>) record) {
                sRecords.add(String.valueOf(rec));
            }
            result = new AbstractMap.SimpleEntry<>(name, sRecords);
        } else {
            List<Object> sRecords = new ArrayList<>();
            for (Object rec : (GenericArray<Object>) record) {
                sRecords.add(translatePrimitiveTypeObject(rec, avroSchema.getElementType(), "").getValue());
            }
            result = new AbstractMap.SimpleEntry<>(name, sRecords);

        } // TODO: handle fixed

        return result;
    }

    @SuppressWarnings("unchecked")
    private static TableRow translateRecord(GenericData.Record record) {

        Schema avroSchema = record.getSchema();

        if (avroSchema.getType() != Schema.Type.RECORD) {
            throw new IllegalArgumentException("Object is not a Avro Record type.");
        }

        TableRow fields = new TableRow();
        for (org.apache.avro.Schema.Field avroField: avroSchema.getFields()) {
            if (isPrimitiveType(avroField.schema().getType())) {
                Map.Entry<String, Object> entry = translatePrimitiveTypeObject(record.get(avroField.name()), avroField.schema(), avroField.name());
                fields.put(entry.getKey(), entry.getValue());
            } else if (avroField.schema().getType() == Schema.Type.RECORD) {
                if (record.get(avroField.name()) != null) {
                    fields.put(avroField.name(), translateRecord((GenericData.Record) record.get(avroField.name())));
                }
            } else if (avroField.schema().getType() == Schema.Type.UNION) {
                Map.Entry<String, Object> entry = translateUnionTypeObject(record.get(avroField.name()), avroField.schema(), avroField.name());
                fields.put(entry.getKey(), entry.getValue());
            } else if (avroField.schema().getType() == Schema.Type.FIXED) {
                Map.Entry<String, Object> entry = translateFixedTypeObject(record.get(avroField.name()), avroField.schema(), avroField.name());
                fields.put(entry.getKey(), entry.getValue());
            } else if (avroField.schema().getType() == Schema.Type.MAP) {
                Map.Entry<String, Object> entry = translateMapTypeObject(record.get(avroField.name()), avroField.schema(), avroField.name());
                fields.put(entry.getKey(), entry.getValue());
            } else if (avroField.schema().getType() == Schema.Type.ENUM) {
                Map.Entry<String, Object> entry = translateEnumTypeObject(record.get(avroField.name()), avroField.name());
                fields.put(entry.getKey(), entry.getValue());
            } else if (avroField.schema().getType() == Schema.Type.ARRAY) {
                Map.Entry<String, Object> entry = translateArrayTypeObject(record.get(avroField.name()), avroField.schema(), avroField.name());
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
        return InsertAllRequest.RowToInsert.of(translateRecord((GenericData.Record) avroRecord));
    }

}


