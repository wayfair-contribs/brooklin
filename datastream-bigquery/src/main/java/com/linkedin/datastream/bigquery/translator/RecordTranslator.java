package com.linkedin.datastream.bigquery.translator;

import com.google.cloud.bigquery.InsertAllRequest;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.*;

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

    private static String formatTime(long time, String pattern, boolean microUnit) {
        SimpleDateFormat fmt = new SimpleDateFormat(pattern);
        fmt.setTimeZone(TimeZone.getTimeZone("UTC"));
        if (microUnit) {
            return fmt.format(new Date(time / 1000)) + "." + String.format("%06d", time % 1000000);
        } else {
            return fmt.format(new Date(time)) + "." + String.format("%06d", (time % 1000) * 1000);
        }
    }

    private static Map.Entry<String, Object> translatePrimitiveTypeObject(Object record, Schema avroSchema, String name) {
        Map.Entry<String, Object> result = new AbstractMap.SimpleEntry<>(name, null);
        switch (avroSchema.getType()) {
            case STRING:
                result = new AbstractMap.SimpleEntry<>(name, record);
                break;
            case FLOAT:
            case DOUBLE:
                result = new AbstractMap.SimpleEntry<>(name, record);
                break;
            case INT:
                if (avroSchema.getLogicalType() != null && avroSchema.getLogicalType().getName().toLowerCase().equals("date")) {
                    result = new AbstractMap.SimpleEntry<>(name, new Date((Integer) record));
                    break;
                } else if (avroSchema.getLogicalType() != null && avroSchema.getLogicalType().getName().toLowerCase().equals("time")) {
                    result = new AbstractMap.SimpleEntry<>(name, formatTime((Long) record, "HH:mm:ss", false));
                    break;
                } else if (avroSchema.getLogicalType() != null && avroSchema.getLogicalType().getName().toLowerCase().equals("timestamp")) {
                    result = new AbstractMap.SimpleEntry<>(name, formatTime((Long) record, "yyyy-MM-dd HH:mm:ss", false));
                    break;
                }
            case LONG:
                if (avroSchema.getLogicalType() != null && avroSchema.getLogicalType().getName().toLowerCase().equals("time")) {
                    result = new AbstractMap.SimpleEntry<>(name,formatTime((Long) record, "HH:mm:ss", true));
                    break;
                } else if (avroSchema.getLogicalType() != null && avroSchema.getLogicalType().getName().toLowerCase().equals("timestamp")) {
                    result = new AbstractMap.SimpleEntry<>(name, formatTime((Long) record, "yyyy-MM-dd HH:mm:ss", true));
                    break;
                } else {
                    result = new AbstractMap.SimpleEntry<>(name, record);
                    break;
                }
            case BOOLEAN:
                result = new AbstractMap.SimpleEntry<>(name, record);
                break;
            case BYTES:
                result = new AbstractMap.SimpleEntry<>(name, record);
                break;
        }
        return result;
    }

    private static Map.Entry<String, Object> translateEnumTypeObject(Object record, String name) {
        return new AbstractMap.SimpleEntry<>(name, record);
    }

    private static Map.Entry<String, Object> translateFixedTypeObject(Object record, Schema avroSchema, String name) {
        Map.Entry<String, Object> result;
        if (avroSchema.getLogicalType() != null && avroSchema.getLogicalType().getName().toLowerCase().equals("decimal")) {
            final LogicalTypes.Decimal decimalType = (LogicalTypes.Decimal) avroSchema.getLogicalType();
            result = new AbstractMap.SimpleEntry<>(name, new BigDecimal(new BigInteger(((GenericFixed) record).bytes()), decimalType.getScale()));
        } else {
            result = new AbstractMap.SimpleEntry<>(name, ((GenericFixed) record).bytes());
        }
        return result;
    }

    private static Map.Entry<String, Object> translateUnionTypeObject(Object record, Schema avroSchema, String name) {

        Map.Entry<String, Object> result = new AbstractMap.SimpleEntry<>(name, null);
        if (avroSchema.getTypes().size() == 2 &&
                (avroSchema.getTypes().get(0).getType() == Schema.Type.NULL ||
                        avroSchema.getTypes().get(1).getType() == Schema.Type.NULL)
        ) {
            if (isPrimitiveType(avroSchema.getTypes().get(0).getType()) ||
                    isPrimitiveType(avroSchema.getTypes().get(1).getType())) {
                result = new AbstractMap.SimpleEntry<>(name, record);
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
                result = new AbstractMap.SimpleEntry<>(Schema.Type.STRING.name().toLowerCase() + "_value", record);
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
                subRecords.put("value", translateRecord((GenericRecord) entry.getValue(), avroSchema.getValueType(), avroSchema.getValueType().getName()));
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
            List<Map.Entry<String, Object>> sRecords = new ArrayList<>();
            for (GenericRecord rec : (GenericArray<GenericRecord>) record) {
                sRecords.add(translateRecord(rec, avroSchema.getElementType(), name));
            }
            result = new AbstractMap.SimpleEntry<>(name, sRecords);
        } else {
            result = new AbstractMap.SimpleEntry<>(name, record);
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private static Map.Entry<String, Object> translateRecord(GenericRecord avroRecord, Schema avroSchema, String name) {
        Map.Entry<String, Object> record = new AbstractMap.SimpleEntry<>(name, null);
        Map<String, Object> subRecords = new HashMap<>();
        SimpleDateFormat fmt = null;

        switch (avroSchema.getType()) {
            case RECORD:
                for (org.apache.avro.Schema.Field avroField: avroSchema.getFields()) {
                    if (isPrimitiveType(avroField.schema().getType())) {
                        Map.Entry<String, Object> entry = translatePrimitiveTypeObject(avroRecord.get(avroField.name()), avroField.schema(), avroField.name());
                        subRecords.put(entry.getKey(), entry.getValue());
                    } else if (avroField.schema().getType() == Schema.Type.RECORD) {
                        if (avroRecord.get(avroField.name()) != null) {
                            Map.Entry<String, Object> subRecord =
                                    translateRecord((GenericRecord) avroRecord.get(avroField.name()),
                                            avroField.schema(),
                                            avroField.name());
                            if (subRecord != null) {
                                subRecords.put(subRecord.getKey(), subRecord.getValue());
                            }
                        }
                    } else if (avroField.schema().getType() == Schema.Type.UNION) {
                        Map.Entry<String, Object> entry = translateUnionTypeObject(avroRecord.get(avroField.name()), avroField.schema(), avroField.name());
                        subRecords.put(entry.getKey(), entry.getValue());
                    } else if (avroField.schema().getType() == Schema.Type.FIXED) {
                        Map.Entry<String, Object> entry = translateFixedTypeObject(avroRecord.get(avroField.name()), avroField.schema(), avroField.name());
                        subRecords.put(entry.getKey(), entry.getValue());
                    } else if (avroField.schema().getType() == Schema.Type.MAP) {
                        Map.Entry<String, Object> entry = translateMapTypeObject(avroRecord.get(avroField.name()), avroField.schema(), avroField.name());
                        subRecords.put(entry.getKey(), entry.getValue());
                    } else if (avroField.schema().getType() == Schema.Type.ENUM) {
                        Map.Entry<String, Object> entry = translateEnumTypeObject(avroRecord.get(avroField.name()), avroField.name());
                        subRecords.put(entry.getKey(), entry.getValue());
                    } else if (avroField.schema().getType() == Schema.Type.ARRAY) {
                        Map.Entry<String, Object> entry = translateArrayTypeObject(avroRecord.get(avroField.name()), avroField.schema(), avroField.name());
                        subRecords.put(entry.getKey(), entry.getValue());
                    }
                }
                record = new AbstractMap.SimpleEntry<>(name, subRecords);
                break;
        }
        return record;
    }

    public static InsertAllRequest.RowToInsert translate(GenericRecord avroRecord, Schema avroSchema) {
        if (avroSchema.getType() != org.apache.avro.Schema.Type.RECORD) {
            throw new IllegalArgumentException("The root of the record's schema should be a RECORD type.");
        }

        Map<String, Object> result = new HashMap<>();

        Map.Entry<String, Object> rec = translateRecord(avroRecord, avroSchema, avroSchema.getName());

        result.put(rec.getKey(), rec.getValue());

        return InsertAllRequest.RowToInsert.of(
                result
        );
    }

}


