/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */

package com.linkedin.datastream.bigquery.translator;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.StandardSQLTypeName;

import static com.google.cloud.bigquery.Field.Mode.NULLABLE;
import static com.google.cloud.bigquery.Field.Mode.REPEATED;
import static com.google.cloud.bigquery.Field.Mode.REQUIRED;

import static com.google.cloud.bigquery.Field.of;


/**
 * Tests for {@link SchemaTranslator}.
 */

public class TestSchemaTranslator {

    /**
     * Primitive Types
     */

    @Test
    public void testBooleanSchema() {
        Schema boolRecord = SchemaBuilder
                .record("user").doc("bbq_avro").namespace("com.wayfair.bbq")
                .fields()
                .name("subscriber_category").doc("Membership Status").type().nullable().booleanType().noDefault()
                .endRecord();

        com.google.cloud.bigquery.Schema schema = com.google.cloud.bigquery.Schema.of(
                Field.newBuilder("subscriber_category", StandardSQLTypeName.BOOL).setMode(NULLABLE).build());

        Assert.assertEquals(schema, SchemaTranslator.translate(boolRecord));
        Assert.assertEquals(SchemaTranslator.translate(boolRecord).getFields().size(), 1);
        Assert.assertEquals(SchemaTranslator.translate(boolRecord).getFields().get(0).getName(), "subscriber_category");
        Assert.assertEquals(SchemaTranslator.translate(boolRecord).getFields().get(0).getType(), LegacySQLTypeName.BOOLEAN);
        Assert.assertEquals(SchemaTranslator.translate(boolRecord).getFields().get(0).getMode(), NULLABLE);
    }

    @Test
    public void testIntSchema() {
        Schema intRecord = SchemaBuilder
                .record("user").doc("bbq_avro").namespace("com.wayfair.bbq")
                .fields()
                .name("subscriber_id").doc("Account Number").type().nullable().intType().noDefault()
                .endRecord();

        com.google.cloud.bigquery.Schema schema = com.google.cloud.bigquery.Schema.of(
                Field.newBuilder("subscriber_id", StandardSQLTypeName.INT64).setMode(NULLABLE).build());

        Assert.assertEquals(schema, SchemaTranslator.translate(intRecord));
        Assert.assertEquals(SchemaTranslator.translate(intRecord).getFields().size(), 1);
        Assert.assertEquals(SchemaTranslator.translate(intRecord).getFields().get(0).getName(), "subscriber_id");
        Assert.assertEquals(SchemaTranslator.translate(intRecord).getFields().get(0).getType(), LegacySQLTypeName.INTEGER);
        Assert.assertEquals(SchemaTranslator.translate(intRecord).getFields().get(0).getMode(), NULLABLE);
    }

    @Test
    public void testLongSchema() {
        Schema longRecord = SchemaBuilder
                .record("user").doc("bbq_avro").namespace("com.wayfair.bbq")
                .fields()
                .name("subscriber_transaction_id").doc("Transaction ID").type().nullable().longType().noDefault()
                .endRecord();

        com.google.cloud.bigquery.Schema schema = com.google.cloud.bigquery.Schema.of(
                Field.newBuilder("subscriber_transaction_id", StandardSQLTypeName.INT64).setMode(NULLABLE).build());

        Assert.assertEquals(schema, SchemaTranslator.translate(longRecord));
        Assert.assertEquals(SchemaTranslator.translate(longRecord).getFields().size(), 1);
        Assert.assertEquals(SchemaTranslator.translate(longRecord).getFields().get(0).getName(), "subscriber_transaction_id");
        Assert.assertEquals(SchemaTranslator.translate(longRecord).getFields().get(0).getType(), LegacySQLTypeName.INTEGER);
        Assert.assertEquals(SchemaTranslator.translate(longRecord).getFields().get(0).getMode(), NULLABLE);
    }

    @Test
    public void testFloatSchema() {
        Schema floatRecord = SchemaBuilder
                .record("user").doc("bbq_avro").namespace("com.wayfair.bbq")
                .fields()
                .name("product_price").doc("Item Price").type().nullable().floatType().noDefault()
                .endRecord();

        com.google.cloud.bigquery.Schema schema = com.google.cloud.bigquery.Schema.of(
                Field.newBuilder("product_price", StandardSQLTypeName.FLOAT64).setMode(NULLABLE).build());

        Assert.assertEquals(schema, SchemaTranslator.translate(floatRecord));
        Assert.assertEquals(SchemaTranslator.translate(floatRecord).getFields().size(), 1);
        Assert.assertEquals(SchemaTranslator.translate(floatRecord).getFields().get(0).getName(), "product_price");
        Assert.assertEquals(SchemaTranslator.translate(floatRecord).getFields().get(0).getType(), LegacySQLTypeName.FLOAT);
        Assert.assertEquals(SchemaTranslator.translate(floatRecord).getFields().get(0).getMode(), NULLABLE);

    }

    @Test
    public void testDoubleSchema() {
        Schema doubleRecord = SchemaBuilder
                .record("user").doc("bbq_avro").namespace("com.wayfair.bbq")
                .fields()
                .name("transaction_amount").doc("Transaction Amount").type().nullable().doubleType().noDefault()
                .endRecord();

        com.google.cloud.bigquery.Schema schema = com.google.cloud.bigquery.Schema.of(
                Field.newBuilder("transaction_amount", StandardSQLTypeName.FLOAT64).setMode(NULLABLE).build());

        Assert.assertEquals(schema, SchemaTranslator.translate(doubleRecord));
        Assert.assertEquals(SchemaTranslator.translate(doubleRecord).getFields().size(), 1);
        Assert.assertEquals(SchemaTranslator.translate(doubleRecord).getFields().get(0).getName(), "transaction_amount");
        Assert.assertEquals(SchemaTranslator.translate(doubleRecord).getFields().get(0).getType(), LegacySQLTypeName.FLOAT);
        Assert.assertEquals(SchemaTranslator.translate(doubleRecord).getFields().get(0).getMode(), NULLABLE);

    }

    @Test
    public void testStringSchema() {
        Schema strRecord = SchemaBuilder
                .record("user").doc("bbq_avro").namespace("com.wayfair.bbq")
                .fields()
                .name("subscriber_name").doc("Account Holder Name").type().nullable().stringType().noDefault()
                .endRecord();

        com.google.cloud.bigquery.Schema schema = com.google.cloud.bigquery.Schema.of(
                Field.newBuilder("subscriber_name", StandardSQLTypeName.STRING).setMode(NULLABLE).build());

        Assert.assertEquals(schema, SchemaTranslator.translate(strRecord));
        Assert.assertEquals(SchemaTranslator.translate(strRecord).getFields().size(), 1);
        Assert.assertEquals(SchemaTranslator.translate(strRecord).getFields().get(0).getName(), "subscriber_name");
        Assert.assertEquals(SchemaTranslator.translate(strRecord).getFields().get(0).getType(), LegacySQLTypeName.STRING);
        Assert.assertEquals(SchemaTranslator.translate(strRecord).getFields().get(0).getMode(), NULLABLE);
    }

    @Test
    public void testByteSchema() {
        Schema byteRecord = SchemaBuilder
                .record("user").doc("bbq_avro").namespace("com.wayfair.bbq")
                .fields()
                .name("sku_category").doc("Item ID").type().nullable().bytesType().noDefault()
                .endRecord();

        com.google.cloud.bigquery.Schema schema = com.google.cloud.bigquery.Schema.of(
                Field.newBuilder("sku_category", StandardSQLTypeName.BYTES).setMode(NULLABLE).build());

        Assert.assertEquals(schema, SchemaTranslator.translate(byteRecord));
        Assert.assertEquals(SchemaTranslator.translate(byteRecord).getFields().size(), 1);
        Assert.assertEquals(SchemaTranslator.translate(byteRecord).getFields().get(0).getName(), "sku_category");
        Assert.assertEquals(SchemaTranslator.translate(byteRecord).getFields().get(0).getType(), LegacySQLTypeName.BYTES);
        Assert.assertEquals(SchemaTranslator.translate(byteRecord).getFields().get(0).getMode(), NULLABLE);
    }

    /**
     * Enums
     */

    @Test
    public void testEnumerationSchema() {
        List<String> enumElements = new ArrayList<String>();
        for (int i = 1; i <= 1000; i++) {
            enumElements.add("S" + i);
        }
        ;
        String[] enumElementsArray = enumElements.toArray(new String[0]);

        Schema enumType1 = SchemaBuilder.enumeration("CEnum1").symbols(enumElementsArray[0]);
        Schema enumType2 = SchemaBuilder.enumeration("CEnum2").symbols(enumElementsArray[1]);
        Schema enumRecord = SchemaBuilder.builder()
                .record("test_enum_types").fields()
                .name("String_Array_Enum1").type(enumType1).noDefault()
                .name("String_Array_Enum2").type().optional().type(enumType2)
                .endRecord();

        com.google.cloud.bigquery.Schema schema = com.google.cloud.bigquery.Schema.of(
                Field.newBuilder("String_Array_Enum1", StandardSQLTypeName.STRING).setMode(REQUIRED).build(),
                Field.newBuilder("String_Array_Enum2", StandardSQLTypeName.STRING).setMode(NULLABLE).build());

        Assert.assertEquals(schema, SchemaTranslator.translate(enumRecord));
        Assert.assertEquals(SchemaTranslator.translate(enumRecord).getFields().size(), 2);
        Assert.assertEquals(SchemaTranslator.translate(enumRecord).getFields().get(0).getName(), "String_Array_Enum1");
        Assert.assertEquals(SchemaTranslator.translate(enumRecord).getFields().get(0).getType(), LegacySQLTypeName.STRING);
        Assert.assertEquals(SchemaTranslator.translate(enumRecord).getFields().get(0).getMode(), REQUIRED);

    }

    /**
     * Arrays
     */

    @Test
    public void testPrimitiveArraysSchema() {
        Schema arrayType1 = SchemaBuilder.array().items().booleanType();
        Schema arrayType2 = SchemaBuilder.array().items().intType();
        Schema arrayType3 = SchemaBuilder.array().items().longType();
        Schema arrayType4 = SchemaBuilder.array().items().floatType();
        Schema arrayType5 = SchemaBuilder.array().items().doubleType();
        Schema arrayType6 = SchemaBuilder.array().items().bytesType();
        Schema arrayType7 = SchemaBuilder.array().items().stringType();
        Schema arrayRecord = SchemaBuilder.builder()
                .record("test_array_types").fields()
                .name("Boolean_Array").type(arrayType1).noDefault()
                .name("Int_Array").type().optional().type(arrayType2)
                .name("Long_Array").type(arrayType3).noDefault()
                .name("Float_Array").type().optional().type(arrayType4)
                .name("Double_Array").type(arrayType5).noDefault()
                .name("Bytes_Array").type().optional().type(arrayType6)
                .name("String_Array").type().optional().type(arrayType7)   // Nested Arrays are NOT allowed
                .endRecord();

        com.google.cloud.bigquery.Schema schemaArray;
        schemaArray = com.google.cloud.bigquery.Schema.of(
                Field.newBuilder("Boolean_Array", StandardSQLTypeName.BOOL).setMode(REPEATED).build(),
                Field.newBuilder("Int_Array", StandardSQLTypeName.INT64).setMode(REPEATED).build(),
                Field.newBuilder("Long_Array", StandardSQLTypeName.INT64).setMode(REPEATED).build(),
                Field.newBuilder("Float_Array", StandardSQLTypeName.FLOAT64).setMode(REPEATED).build(),
                Field.newBuilder("Double_Array", StandardSQLTypeName.FLOAT64).setMode(REPEATED).build(),
                Field.newBuilder("Bytes_Array", StandardSQLTypeName.BYTES).setMode(REPEATED).build(),
                Field.newBuilder("String_Array", StandardSQLTypeName.STRING).setMode(REPEATED).build()
        );

        Assert.assertEquals(schemaArray.toString(), SchemaTranslator.translate(arrayRecord).toString());
        Assert.assertEquals(SchemaTranslator.translate(arrayRecord).getFields().size(), 7);
        Assert.assertEquals(SchemaTranslator.translate(arrayRecord).getFields().get(0).getName(), "Boolean_Array");
        Assert.assertEquals(SchemaTranslator.translate(arrayRecord).getFields().get(0).getType(), LegacySQLTypeName.BOOLEAN);
        Assert.assertEquals(SchemaTranslator.translate(arrayRecord).getFields().get(0).getMode(), REPEATED);
    }

    @Test
    public void testEnumArraysSchema() {
        List<String> enumElements = new ArrayList<String>();
        for (int i = 1; i <= 1000; i++) {
            enumElements.add("S" + i);
        }
        ;
        String[] enumElementsArray = enumElements.toArray(new String[0]);

        Schema arrayEnumType = SchemaBuilder.array().items().enumeration("CEnum1").symbols(enumElementsArray[0]);
        Schema arrayRecordEnum = SchemaBuilder.builder()
                .record("test_arrayofenum_types").fields()
                .name("Enum_String_Array").type(arrayEnumType).noDefault()
                .endRecord();
        Schema enumArrayRecord = SchemaBuilder.builder()
                .record("test_arrayofenum_types").fields()
                .name("Enum_String_Array").type(arrayRecordEnum).noDefault()
                .endRecord();
        com.google.cloud.bigquery.Schema enumSchemaArray = com.google.cloud.bigquery.Schema.of(
                Field.newBuilder("Enum_String_Array", StandardSQLTypeName.STRUCT,
                        of("string", StandardSQLTypeName.STRING)).setMode(REQUIRED).build());
        Assert.assertEquals(enumSchemaArray.toString(), SchemaTranslator.translate(enumArrayRecord).toString());
        Assert.assertEquals(SchemaTranslator.translate(enumArrayRecord).getFields().size(), 1);
        Assert.assertEquals(SchemaTranslator.translate(enumArrayRecord).getFields().get(0).getName(), "Enum_String_Array");
        Assert.assertEquals(SchemaTranslator.translate(enumArrayRecord).getFields().get(0).getType(), LegacySQLTypeName.RECORD);
        Assert.assertEquals(SchemaTranslator.translate(enumArrayRecord).getFields().get(0).getMode(), REQUIRED);

    }

    @Test
    public void testMapArraysSchema() {
        Schema mapArrayType = SchemaBuilder.map().values().array().items().stringType();
        Schema arrayRecordMap = SchemaBuilder.builder()
                .record("test_arrayofmap_types").fields()
                .name("Map_String_Array").type(mapArrayType).noDefault()
                .endRecord();
        Schema mapArrayRecord = SchemaBuilder.builder()
                .record("test_arrayofmap_types").fields()
                .name("Map_String_Array").type(arrayRecordMap).noDefault()
                .endRecord();
        com.google.cloud.bigquery.Schema mapSchemaArray = com.google.cloud.bigquery.Schema.of(
                Field.newBuilder("Map_String_Array", StandardSQLTypeName.STRUCT,
                        of("string", StandardSQLTypeName.STRING)).setMode(REQUIRED).build());
        Assert.assertEquals(mapSchemaArray.toString(), SchemaTranslator.translate(mapArrayRecord).toString());
        Assert.assertEquals(SchemaTranslator.translate(mapArrayRecord).getFields().size(), 1);
        Assert.assertEquals(SchemaTranslator.translate(mapArrayRecord).getFields().get(0).getName(), "Map_String_Array");
        Assert.assertEquals(SchemaTranslator.translate(mapArrayRecord).getFields().get(0).getType(), LegacySQLTypeName.RECORD);
        Assert.assertEquals(SchemaTranslator.translate(mapArrayRecord).getFields().get(0).getMode(), REQUIRED);

    }

    @Test
    public void testUnionArraysSchema() {
        Schema unionType = SchemaBuilder.unionOf().array().items().stringType().endUnion();
        Schema arrayRecordUnion = SchemaBuilder.builder()
                .record("test_arrayofmap_types").fields()
                .name("Map_String_Array").type(unionType).noDefault()
                .endRecord();
        Schema unionArrayRecord = SchemaBuilder.builder()
                .record("test_arrayofmap_types").fields()
                .name("Map_String_Array").type(arrayRecordUnion).noDefault()
                .endRecord();
        com.google.cloud.bigquery.Schema unionSchemaArray = com.google.cloud.bigquery.Schema.of(
                Field.newBuilder("Map_String_Array", StandardSQLTypeName.STRUCT,
                        of("string", StandardSQLTypeName.STRING)).setMode(REQUIRED).build());
        Assert.assertEquals(unionSchemaArray.toString(), SchemaTranslator.translate(unionArrayRecord).toString());
        Assert.assertEquals(SchemaTranslator.translate(unionArrayRecord).getFields().size(), 1);
        Assert.assertEquals(SchemaTranslator.translate(unionArrayRecord).getFields().get(0).getName(), "Map_String_Array");
        Assert.assertEquals(SchemaTranslator.translate(unionArrayRecord).getFields().get(0).getType(), LegacySQLTypeName.RECORD);
        Assert.assertEquals(SchemaTranslator.translate(unionArrayRecord).getFields().get(0).getMode(), REQUIRED);

    }

    @Test
    public void testRecordArraysSchema() {
        Schema recordType = SchemaBuilder.record("stringRecord").fields().name("stringRecord").doc("stringRecord").type().nullable().stringType().noDefault().endRecord();
        Schema arrayRecordRecord = SchemaBuilder.builder()
                .record("test_arrayofmap_types").fields()
                .name("Map_String_Array").type(recordType).noDefault()
                .endRecord();
        Schema recordArrayRecord = SchemaBuilder.builder()
                .record("test_arrayofmap_types").fields()
                .name("Map_String_Array").type(arrayRecordRecord).noDefault()
                .endRecord();
        com.google.cloud.bigquery.Schema recordSchemaArray = com.google.cloud.bigquery.Schema.of(
                Field.newBuilder("Map_String_Array", StandardSQLTypeName.STRUCT,
                        of("string", StandardSQLTypeName.STRING)).setMode(REQUIRED).build());
        Assert.assertEquals(recordSchemaArray.toString(), SchemaTranslator.translate(recordArrayRecord).toString());
        Assert.assertEquals(SchemaTranslator.translate(recordArrayRecord).getFields().size(), 1);
        Assert.assertEquals(SchemaTranslator.translate(recordArrayRecord).getFields().get(0).getName(), "Map_String_Array");
        Assert.assertEquals(SchemaTranslator.translate(recordArrayRecord).getFields().get(0).getType(), LegacySQLTypeName.RECORD);
        Assert.assertEquals(SchemaTranslator.translate(recordArrayRecord).getFields().get(0).getMode(), REQUIRED);

    }

    /**
     * Maps
     */

    @Test
    public void testPrimitiveMapSchema() {
        Schema mapType1 = SchemaBuilder.map().values().booleanType();
        Schema mapType2 = SchemaBuilder.map().values().intType();
        Schema mapType3 = SchemaBuilder.map().values().longType();
        Schema mapType4 = SchemaBuilder.map().values().floatType();
        Schema mapType5 = SchemaBuilder.map().values().doubleType();
        Schema mapType6 = SchemaBuilder.map().values().bytesType();
        Schema mapType7 = SchemaBuilder.map().values(SchemaBuilder.map().values().stringType());
        Schema mapRecord = SchemaBuilder.builder()
                .record("test_map_types").fields()
                .name("Boolean_Map").type(mapType1).noDefault()
                .name("Int_Map").type().optional().type(mapType2)
                .name("Long_Map").type().optional().type(mapType3)
                .name("Float_Map").type().optional().type(mapType4)
                .name("Double_Map").type().optional().type(mapType5)
                .name("Bytes_Map").type().optional().type(mapType6)
                .name("String_Map").type().optional().type(mapType7)
                .endRecord();

        com.google.cloud.bigquery.Schema schemaMap = com.google.cloud.bigquery.Schema.of(
                Field.newBuilder("Boolean_Map", StandardSQLTypeName.STRUCT,
                        of("boolean", StandardSQLTypeName.BOOL)).setMode(REPEATED).build(),
                Field.newBuilder("Int_Map", StandardSQLTypeName.STRUCT,
                        of("int", StandardSQLTypeName.INT64)).setMode(REPEATED).build(),
                Field.newBuilder("Long_Map", StandardSQLTypeName.STRUCT,
                        of("long", StandardSQLTypeName.INT64)).setMode(REPEATED).build(),
                Field.newBuilder("Float_Map", StandardSQLTypeName.STRUCT,
                        of("float", StandardSQLTypeName.FLOAT64)).setMode(REPEATED).build(),
                Field.newBuilder("Double_Map", StandardSQLTypeName.STRUCT,
                        of("double", StandardSQLTypeName.FLOAT64)).setMode(REPEATED).build(),
                Field.newBuilder("Bytes_Map", StandardSQLTypeName.STRUCT,
                        of("bytes", StandardSQLTypeName.BYTES)).setMode(REPEATED).build(),
                Field.newBuilder("String_Map", StandardSQLTypeName.STRUCT,
                        of("string", StandardSQLTypeName.STRING)).setMode(REPEATED).build());

        Assert.assertEquals(schemaMap.toString(), SchemaTranslator.translate(mapRecord).toString());
        Assert.assertEquals(SchemaTranslator.translate(mapRecord).getFields().size(), 7);
        Assert.assertEquals(SchemaTranslator.translate(mapRecord).getFields().get(0).getName(), "Boolean_Map");
        Assert.assertEquals(SchemaTranslator.translate(mapRecord).getFields().get(0).getType(), LegacySQLTypeName.RECORD);
        Assert.assertEquals(SchemaTranslator.translate(mapRecord).getFields().get(0).getMode(), REPEATED);

    }

    @Test
    public void testEnumMapSchema() {
        List<String> enumElements = new ArrayList<String>();
        for (int i = 1; i <= 1000; i++) {
            enumElements.add("S"+i);
        };
        String[] enumElementsArray = enumElements.toArray(new String[0]);
        Schema mapEnumType = SchemaBuilder.map().values().enumeration("CEnum1").symbols(enumElementsArray[0]);
        Schema enumMapRecord = SchemaBuilder.builder()
                .record("test_mapofenumofstring_types").fields()
                .name("Enum_String_Map").type(mapEnumType).noDefault()
                .endRecord();
        com.google.cloud.bigquery.Schema enumSchemaMap = com.google.cloud.bigquery.Schema.of(
                Field.newBuilder("Enum_String_Map", StandardSQLTypeName.STRUCT,
                        of("string", StandardSQLTypeName.STRING)).setMode(REPEATED).build());
        Assert.assertEquals(enumSchemaMap.toString(), SchemaTranslator.translate(enumMapRecord).toString());
        Assert.assertEquals(SchemaTranslator.translate(enumMapRecord).getFields().size(), 1);
        Assert.assertEquals(SchemaTranslator.translate(enumMapRecord).getFields().get(0).getName(), "Enum_String_Map");
        Assert.assertEquals(SchemaTranslator.translate(enumMapRecord).getFields().get(0).getType(), LegacySQLTypeName.RECORD);
        Assert.assertEquals(SchemaTranslator.translate(enumMapRecord).getFields().get(0).getMode(), REPEATED);

    }

    @Test
    public void testArrayMapSchema() {
        Schema mapArrayType = SchemaBuilder.map().values().array().items().stringType();
        Schema mapArrayRecord = SchemaBuilder.builder()
                .record("test_mapofenumofstring_types").fields()
                .name("Enum_String_Map").type(mapArrayType).noDefault()
                .endRecord();
        com.google.cloud.bigquery.Schema arraySchemaMap = com.google.cloud.bigquery.Schema.of(
                Field.newBuilder("Enum_String_Map", StandardSQLTypeName.STRUCT,
                        of("string", StandardSQLTypeName.STRING)).setMode(REPEATED).build());
        Assert.assertEquals(arraySchemaMap.toString(), SchemaTranslator.translate(mapArrayRecord).toString());
        Assert.assertEquals(SchemaTranslator.translate(mapArrayRecord).getFields().size(), 1);
        Assert.assertEquals(SchemaTranslator.translate(mapArrayRecord).getFields().get(0).getName(), "Enum_String_Map");
        Assert.assertEquals(SchemaTranslator.translate(mapArrayRecord).getFields().get(0).getType(), LegacySQLTypeName.RECORD);
        Assert.assertEquals(SchemaTranslator.translate(mapArrayRecord).getFields().get(0).getMode(), REPEATED);

    }

    @Test
    public void testMapMapSchema() {
        Schema mapType = SchemaBuilder.map().values().stringType();
        Schema mapMapType = SchemaBuilder.map().values(mapType);
        Schema mapMapRecord = SchemaBuilder.builder()
                .record("test_mapofenumofstring_types").fields()
                .name("Enum_String_Map").type(mapMapType).noDefault()
                .endRecord();
        com.google.cloud.bigquery.Schema mapSchemaMap = com.google.cloud.bigquery.Schema.of(
                Field.newBuilder("Enum_String_Map", StandardSQLTypeName.STRUCT,
                        of("string", StandardSQLTypeName.STRING)).setMode(REPEATED).build());
        Assert.assertEquals(mapSchemaMap.toString(), SchemaTranslator.translate(mapMapRecord).toString());
        Assert.assertEquals(SchemaTranslator.translate(mapMapRecord).getFields().size(), 1);
        Assert.assertEquals(SchemaTranslator.translate(mapMapRecord).getFields().get(0).getName(), "Enum_String_Map");
        Assert.assertEquals(SchemaTranslator.translate(mapMapRecord).getFields().get(0).getType(), LegacySQLTypeName.RECORD);
        Assert.assertEquals(SchemaTranslator.translate(mapMapRecord).getFields().get(0).getMode(), REPEATED);

    }

    @Test
    public void testUnionMapSchema() {
        Schema unionType = SchemaBuilder.unionOf().array().items().stringType().endUnion();
        Schema mapUnionType = SchemaBuilder.map().values(unionType);
        Schema mapUnionRecord = SchemaBuilder.builder()
                .record("test_mapofenumofstring_types").fields()
                .name("Enum_String_Map").type(mapUnionType).noDefault()
                .endRecord();
        com.google.cloud.bigquery.Schema unionSchemaMap = com.google.cloud.bigquery.Schema.of(
                Field.newBuilder("Enum_String_Map", StandardSQLTypeName.STRUCT,
                        of("string", StandardSQLTypeName.STRING)).setMode(REPEATED).build());
        Assert.assertEquals(unionSchemaMap.toString(), SchemaTranslator.translate(mapUnionRecord).toString());
        Assert.assertEquals(SchemaTranslator.translate(mapUnionRecord).getFields().size(), 1);
        Assert.assertEquals(SchemaTranslator.translate(mapUnionRecord).getFields().get(0).getName(), "Enum_String_Map");
        Assert.assertEquals(SchemaTranslator.translate(mapUnionRecord).getFields().get(0).getType(), LegacySQLTypeName.RECORD);
        Assert.assertEquals(SchemaTranslator.translate(mapUnionRecord).getFields().get(0).getMode(), REPEATED);

    }

    @Test
    public void testRecordMapSchema() {
        Schema recordType = SchemaBuilder.record("stringRecord").fields().name("stringRecord").doc("stringRecord").type().nullable().stringType().noDefault().endRecord();
        Schema mapRecordType = SchemaBuilder.map().values(recordType);
        Schema mapRecordRecord = SchemaBuilder.builder()
                .record("test_mapofenumofstring_types").fields()
                .name("Enum_String_Map").type(mapRecordType).noDefault()
                .endRecord();
        com.google.cloud.bigquery.Schema recordSchemaMap = com.google.cloud.bigquery.Schema.of(
                Field.newBuilder("Enum_String_Map", StandardSQLTypeName.STRUCT,
                        of("string", StandardSQLTypeName.STRING)).setMode(REPEATED).build());
        Assert.assertEquals(recordSchemaMap.toString(), SchemaTranslator.translate(mapRecordRecord).toString());
        Assert.assertEquals(SchemaTranslator.translate(mapRecordRecord).getFields().size(), 1);
        Assert.assertEquals(SchemaTranslator.translate(mapRecordRecord).getFields().get(0).getName(), "Enum_String_Map");
        Assert.assertEquals(SchemaTranslator.translate(mapRecordRecord).getFields().get(0).getType(), LegacySQLTypeName.RECORD);
        Assert.assertEquals(SchemaTranslator.translate(mapRecordRecord).getFields().get(0).getMode(), REPEATED);

    }

    /**
     * Unions
     */

    @Test
    public void testPrimitiveUnionSchema() {
        Schema unionType1 = SchemaBuilder.unionOf().map().values().booleanType().endUnion();
        Schema unionType2 = SchemaBuilder.unionOf().array().items().intType().endUnion();
        Schema unionType3 = SchemaBuilder.unionOf().map().values().longType().endUnion();
        Schema unionType4 = SchemaBuilder.unionOf().array().items().floatType().endUnion();
        Schema unionType5 = SchemaBuilder.unionOf().map().values().doubleType().endUnion();
        Schema unionType6 = SchemaBuilder.unionOf().array().items().bytesType().endUnion();
        Schema unionType7 = SchemaBuilder.unionOf().map().values().stringType().endUnion();
        Schema unionRecord = SchemaBuilder.builder()
                .record("test_union_types").fields()
                .name("Boolean_Union").type(unionType1).noDefault()
                .name("Int_Union").type(unionType2).noDefault()
                .name("Long_Union").type(unionType3).noDefault()
                .name("Float_Union").type(unionType4).noDefault()
                .name("Double_Union").type(unionType5).noDefault()
                .name("Bytes_Union").type(unionType6).noDefault()
                .name("String_Union").type(unionType7).noDefault()
                .endRecord();

        com.google.cloud.bigquery.Schema schemaUnion = com.google.cloud.bigquery.Schema.of(
                Field.newBuilder("Boolean_Union", StandardSQLTypeName.STRUCT,
                        of("bool", StandardSQLTypeName.BOOL)).setMode(REQUIRED).build(),
                Field.newBuilder("Int_Union", StandardSQLTypeName.STRUCT,
                        of("int", StandardSQLTypeName.INT64)).setMode(REQUIRED).build(),
                Field.newBuilder("Long_Union", StandardSQLTypeName.STRUCT,
                        of("long", StandardSQLTypeName.INT64)).setMode(REQUIRED).build(),
                Field.newBuilder("Float_Union", StandardSQLTypeName.STRUCT,
                        of("float", StandardSQLTypeName.FLOAT64)).setMode(REQUIRED).build(),
                Field.newBuilder("Double_Union", StandardSQLTypeName.STRUCT,
                        of("double", StandardSQLTypeName.FLOAT64)).setMode(REQUIRED).build(),
                Field.newBuilder("Bytes_Union", StandardSQLTypeName.STRUCT,
                        of("bytes", StandardSQLTypeName.BYTES)).setMode(REQUIRED).build(),
                Field.newBuilder("String_Union", StandardSQLTypeName.STRUCT,
                        of("string", StandardSQLTypeName.STRING)).setMode(REQUIRED).build());

        Assert.assertEquals(schemaUnion.toString(), SchemaTranslator.translate(unionRecord).toString());
        Assert.assertEquals(SchemaTranslator.translate(unionRecord).getFields().size(), 7);
        Assert.assertEquals(SchemaTranslator.translate(unionRecord).getFields().get(0).getName(), "Boolean_Union");
        Assert.assertEquals(SchemaTranslator.translate(unionRecord).getFields().get(0).getType(), LegacySQLTypeName.RECORD);
        Assert.assertEquals(SchemaTranslator.translate(unionRecord).getFields().get(0).getMode(), REQUIRED);

    }

    @Test
    public void testEnumUnionSchema() {
        List<String> enumElements = new ArrayList<String>();
        for (int i = 1; i <= 1000; i++) {
            enumElements.add("S"+i);
        };
        String[] enumElementsArray = enumElements.toArray(new String[0]);
        Schema unionEnumType = SchemaBuilder.unionOf().enumeration("Enum").symbols(enumElementsArray[0]).endUnion();
        Schema unionEnumRecord = SchemaBuilder.builder()
                .record("test_unionofenumofstring_types").fields()
                .name("Enum_String_Union").type(unionEnumType).noDefault()
                .endRecord();
        com.google.cloud.bigquery.Schema enumSchemaUnion = com.google.cloud.bigquery.Schema.of(
                Field.newBuilder("Enum_String_Union", StandardSQLTypeName.STRUCT,
                        of("string", StandardSQLTypeName.STRING)).setMode(REQUIRED).build());
        Assert.assertEquals(enumSchemaUnion.toString(), SchemaTranslator.translate(unionEnumRecord).toString());
        Assert.assertEquals(SchemaTranslator.translate(unionEnumRecord).getFields().size(), 1);
        Assert.assertEquals(SchemaTranslator.translate(unionEnumRecord).getFields().get(0).getName(), "Enum_String_Union");
        Assert.assertEquals(SchemaTranslator.translate(unionEnumRecord).getFields().get(0).getType(), LegacySQLTypeName.RECORD);
        Assert.assertEquals(SchemaTranslator.translate(unionEnumRecord).getFields().get(0).getMode(), REQUIRED);

    }

    @Test
    public void testArrayUnionSchema() {
        Schema unionArrayType = SchemaBuilder.unionOf().array().items().stringType().endUnion();
        Schema unionArrayRecord = SchemaBuilder.builder()
                .record("test_unionofenumofstring_types").fields()
                .name("Enum_String_Union").type(unionArrayType).noDefault()
                .endRecord();
        com.google.cloud.bigquery.Schema arraySchemaMap = com.google.cloud.bigquery.Schema.of(
                Field.newBuilder("Enum_String_Union", StandardSQLTypeName.STRUCT,
                        of("string", StandardSQLTypeName.STRING)).setMode(REQUIRED).build());
        Assert.assertEquals(arraySchemaMap.toString(), SchemaTranslator.translate(unionArrayRecord).toString());
        Assert.assertEquals(SchemaTranslator.translate(unionArrayRecord).getFields().size(), 1);
        Assert.assertEquals(SchemaTranslator.translate(unionArrayRecord).getFields().get(0).getName(), "Enum_String_Union");
        Assert.assertEquals(SchemaTranslator.translate(unionArrayRecord).getFields().get(0).getType(), LegacySQLTypeName.RECORD);
        Assert.assertEquals(SchemaTranslator.translate(unionArrayRecord).getFields().get(0).getMode(), REQUIRED);

    }

    @Test
    public void testMapUnionSchema() {
        Schema unionMapType = SchemaBuilder.unionOf().map().values().stringType().endUnion();
        Schema unionMapRecord = SchemaBuilder.builder()
                .record("test_unionofenumofstring_types").fields()
                .name("Enum_String_Union").type(unionMapType).noDefault()
                .endRecord();
        com.google.cloud.bigquery.Schema mapSchemaUnion = com.google.cloud.bigquery.Schema.of(
                Field.newBuilder("Enum_String_Union", StandardSQLTypeName.STRUCT,
                        of("string", StandardSQLTypeName.STRING)).setMode(REQUIRED).build());
        Assert.assertEquals(mapSchemaUnion.toString(), SchemaTranslator.translate(unionMapRecord).toString());
        Assert.assertEquals(SchemaTranslator.translate(unionMapRecord).getFields().size(), 1);
        Assert.assertEquals(SchemaTranslator.translate(unionMapRecord).getFields().get(0).getName(), "Enum_String_Union");
        Assert.assertEquals(SchemaTranslator.translate(unionMapRecord).getFields().get(0).getType(), LegacySQLTypeName.RECORD);
        Assert.assertEquals(SchemaTranslator.translate(unionMapRecord).getFields().get(0).getMode(), REQUIRED);

    }

    @Test
    public void testRecordUnionSchema() {
        Schema recordType = SchemaBuilder.record("stringRecord").fields().name("stringRecord").doc("stringRecord").type().nullable().stringType().noDefault().endRecord();
        Schema unionRecordType = SchemaBuilder.unionOf().record("test_recordofstringofunion_types").fields().name("String_Record").type(recordType).noDefault().endRecord().endUnion();
        Schema unionRecordRecord = SchemaBuilder.builder()
                .record("test_union_types").fields()
                .name("Enum_String_Union").type(unionRecordType).noDefault()
                .endRecord();
        com.google.cloud.bigquery.Schema recordSchemaUnion = com.google.cloud.bigquery.Schema.of(
                Field.newBuilder("Enum_String_Union", StandardSQLTypeName.STRUCT,
                        of("string", StandardSQLTypeName.STRING)).setMode(REQUIRED).build());
        Assert.assertEquals(recordSchemaUnion.toString(), SchemaTranslator.translate(unionRecordRecord).toString());
        Assert.assertEquals(SchemaTranslator.translate(unionRecordRecord).getFields().size(), 1);
        Assert.assertEquals(SchemaTranslator.translate(unionRecordRecord).getFields().get(0).getName(), "Enum_String_Union");
        Assert.assertEquals(SchemaTranslator.translate(unionRecordRecord).getFields().get(0).getType(), LegacySQLTypeName.RECORD);
        Assert.assertEquals(SchemaTranslator.translate(unionRecordRecord).getFields().get(0).getMode(), REQUIRED);

    }

    /**
     * Record
     */

    @Test
    public void testPrimitiveRecordSchema() {

        // Record : Record of Primitive types
        Schema recordType1 = SchemaBuilder.record("Boolean").fields().name("Boolean").doc("Boolean").type().nullable().booleanType().noDefault().endRecord();
        Schema recordType2 = SchemaBuilder.record("Int").fields().name("Int").doc("Int").type().nullable().intType().noDefault().endRecord();
        Schema recordType3 = SchemaBuilder.record("Long").fields().name("Long").doc("Long").type().nullable().longType().noDefault().endRecord();
        Schema recordType4 = SchemaBuilder.record("Float").fields().name("Float").doc("Float").type().nullable().floatType().noDefault().endRecord();
        Schema recordType5 = SchemaBuilder.record("Double").fields().name("Double").doc("Double").type().nullable().doubleType().noDefault().endRecord();
        Schema recordType6 = SchemaBuilder.record("Bytes").fields().name("Bytes").doc("Bytes").type().nullable().bytesType().noDefault().endRecord();
        Schema recordType7 = SchemaBuilder.record("String").fields().name("String").doc("String").type().nullable().stringType().noDefault().endRecord();
        Schema recordRecord = SchemaBuilder.builder()
                .record("test_union_types").fields()
                .name("Boolean_Record").type(recordType1).noDefault()
                .name("Int_Record").type(recordType2).noDefault()
                .name("Long_Record").type(recordType3).noDefault()
                .name("Float_Record").type(recordType4).noDefault()
                .name("Double_Record").type(recordType5).noDefault()
                .name("Bytes_Record").type(recordType6).noDefault()
                .name("String_Record").type(recordType7).noDefault()
                .endRecord();

        com.google.cloud.bigquery.Schema schemaRecord;
        schemaRecord = com.google.cloud.bigquery.Schema.of(
                Field.newBuilder("Boolean_Record", LegacySQLTypeName.RECORD,
                        of("bool", StandardSQLTypeName.BOOL)).setMode(REQUIRED).build(),
                Field.newBuilder("Int_Record", LegacySQLTypeName.RECORD,
                        of("int", StandardSQLTypeName.INT64)).setMode(REQUIRED).build(),
                Field.newBuilder("Long_Record", LegacySQLTypeName.RECORD,
                        of("long", StandardSQLTypeName.INT64)).setMode(REQUIRED).build(),
                Field.newBuilder("Float_Record", LegacySQLTypeName.RECORD,
                        of("float", StandardSQLTypeName.FLOAT64)).setMode(REQUIRED).build(),
                Field.newBuilder("Double_Record", LegacySQLTypeName.RECORD,
                        of("double", StandardSQLTypeName.FLOAT64)).setMode(REQUIRED).build(),
                Field.newBuilder("Bytes_Record", LegacySQLTypeName.RECORD,
                        of("bytes", StandardSQLTypeName.BYTES)).setMode(REQUIRED).build(),
                Field.newBuilder("String_Record", LegacySQLTypeName.RECORD,
                        of("string", StandardSQLTypeName.STRING)).setMode(REQUIRED).build());

        Assert.assertEquals(schemaRecord.toString(), SchemaTranslator.translate(recordRecord).toString());
        Assert.assertEquals(SchemaTranslator.translate(recordRecord).getFields().size(), 7);
        Assert.assertEquals(SchemaTranslator.translate(recordRecord).getFields().get(0).getName(), "Boolean_Record");
        Assert.assertEquals(SchemaTranslator.translate(recordRecord).getFields().get(0).getType(), LegacySQLTypeName.RECORD);
        Assert.assertEquals(SchemaTranslator.translate(recordRecord).getFields().get(0).getMode(), REQUIRED);

    }

    @Test
    public void testEnumRecordSchema() {
        List<String> enumElements = new ArrayList<String>();
        for (int i = 1; i <= 1000; i++) {
            enumElements.add("S"+i);
        };
        String[] enumElementsArray = enumElements.toArray(new String[0]);
        Schema enumType = SchemaBuilder.enumeration("CEnum1").symbols(enumElementsArray[0]);
        Schema enumRecordRecord = SchemaBuilder.builder()
                .record("test_mapofrecords_types").fields()
                .name("String_Record").type(enumType).noDefault()
                .endRecord();
        Schema recordEnumRecord = SchemaBuilder.builder()
                .record("test_mapofmapofrecords_types").fields()
                .name("String_Record").type(enumRecordRecord).noDefault()
                .endRecord();
        com.google.cloud.bigquery.Schema enumSchemaRecord = com.google.cloud.bigquery.Schema.of(
                Field.newBuilder("String_Record", LegacySQLTypeName.RECORD,
                        of("string", LegacySQLTypeName.RECORD, of("string", StandardSQLTypeName.STRING))).setMode(REQUIRED).build());
        Assert.assertEquals(enumSchemaRecord.toString(), SchemaTranslator.translate(recordEnumRecord).toString());
        Assert.assertEquals(SchemaTranslator.translate(recordEnumRecord).getFields().size(), 1);
        Assert.assertEquals(SchemaTranslator.translate(recordEnumRecord).getFields().get(0).getName(), "String_Record");
        Assert.assertEquals(SchemaTranslator.translate(recordEnumRecord).getFields().get(0).getType(), LegacySQLTypeName.RECORD);
        Assert.assertEquals(SchemaTranslator.translate(recordEnumRecord).getFields().get(0).getMode(), REQUIRED);

    }

    @Test
    public void testArrayRecordSchema() {
        Schema arrayType = SchemaBuilder.array().items().intType();
        Schema arrayRecordRecord = SchemaBuilder.builder()
                .record("test_arrayofrecords_types").fields()
                .name("Int_Record").type(arrayType).noDefault()
                .endRecord();
        Schema recordArrayRecord = SchemaBuilder.builder()
                .record("test_arrayofarrayofrecords_types").fields()
                .name("Int_Record").type(arrayRecordRecord).noDefault()
                .endRecord();
        com.google.cloud.bigquery.Schema arraySchemaRecord = com.google.cloud.bigquery.Schema.of(
                Field.newBuilder("Int_Record", LegacySQLTypeName.RECORD,
                        of("int", LegacySQLTypeName.RECORD, of("int", StandardSQLTypeName.INT64))).setMode(REQUIRED).build());
        Assert.assertEquals(arraySchemaRecord.toString(), SchemaTranslator.translate(recordArrayRecord).toString());
        Assert.assertEquals(SchemaTranslator.translate(recordArrayRecord).getFields().size(), 1);
        Assert.assertEquals(SchemaTranslator.translate(recordArrayRecord).getFields().get(0).getName(), "Int_Record");
        Assert.assertEquals(SchemaTranslator.translate(recordArrayRecord).getFields().get(0).getType(), LegacySQLTypeName.RECORD);
        Assert.assertEquals(SchemaTranslator.translate(recordArrayRecord).getFields().get(0).getMode(), REQUIRED);

    }

    @Test
    public void testMapRecordSchema() {
        Schema mapType = SchemaBuilder.map().values().intType();
        Schema mapRecordRecord = SchemaBuilder.builder()
                .record("test_mapofrecords_types").fields()
                .name("Int_Record").type(mapType).noDefault()
                .endRecord();
        Schema recordMapRecord = SchemaBuilder.builder()
                .record("test_mapofmapofrecords_types").fields()
                .name("Int_Record").type(mapRecordRecord).noDefault()
                .endRecord();
        com.google.cloud.bigquery.Schema mapSchemaRecord = com.google.cloud.bigquery.Schema.of(
                Field.newBuilder("Int_Record", LegacySQLTypeName.RECORD,
                        of("int", LegacySQLTypeName.RECORD, of("int", StandardSQLTypeName.INT64))).setMode(REQUIRED).build());
        Assert.assertEquals(mapSchemaRecord.toString(), SchemaTranslator.translate(recordMapRecord).toString());
        Assert.assertEquals(SchemaTranslator.translate(recordMapRecord).getFields().size(), 1);
        Assert.assertEquals(SchemaTranslator.translate(recordMapRecord).getFields().get(0).getName(), "Int_Record");
        Assert.assertEquals(SchemaTranslator.translate(recordMapRecord).getFields().get(0).getType(), LegacySQLTypeName.RECORD);
        Assert.assertEquals(SchemaTranslator.translate(recordMapRecord).getFields().get(0).getMode(), REQUIRED);

    }

    @Test
    public void testUnionRecordSchema() {
        Schema unionType = SchemaBuilder.unionOf().array().items().intType().endUnion();
        Schema unionRecordUnion = SchemaBuilder.builder()
                .record("test_mapofrecords_types").fields()
                .name("Int_Record").type(unionType).noDefault()
                .endRecord();
        Schema recordUnionRecord = SchemaBuilder.builder()
                .record("test_mapofmapofrecords_types").fields()
                .name("Int_Record").type(unionRecordUnion).noDefault()
                .endRecord();
        com.google.cloud.bigquery.Schema unionSchemaRecord = com.google.cloud.bigquery.Schema.of(
                Field.newBuilder("Int_Record", LegacySQLTypeName.RECORD,
                        of("int", LegacySQLTypeName.RECORD, of("int", StandardSQLTypeName.INT64))).setMode(REQUIRED).build());
        Assert.assertEquals(unionSchemaRecord.toString(), SchemaTranslator.translate(recordUnionRecord).toString());
        Assert.assertEquals(SchemaTranslator.translate(recordUnionRecord).getFields().size(), 1);
        Assert.assertEquals(SchemaTranslator.translate(recordUnionRecord).getFields().get(0).getName(), "Int_Record");
        Assert.assertEquals(SchemaTranslator.translate(recordUnionRecord).getFields().get(0).getType(), LegacySQLTypeName.RECORD);
        Assert.assertEquals(SchemaTranslator.translate(recordUnionRecord).getFields().get(0).getMode(), REQUIRED);

    }

    @Test
    public void testRecordRecordSchema() {
        Schema recordType = SchemaBuilder.record("stringRecord").fields().name("stringRecord").doc("stringRecord").type().nullable().stringType().noDefault().endRecord();
        Schema unionRecordRecord = SchemaBuilder.builder()
                .record("test_mapofrecords_types").fields()
                .name("Int_Record").type(recordType).noDefault()
                .endRecord();
        Schema recordRecordRecords = SchemaBuilder.builder()
                .record("test_mapofmapofrecords_types").fields()
                .name("Int_Record").type(unionRecordRecord).noDefault()
                .endRecord();
        com.google.cloud.bigquery.Schema recordSchemaRecord = com.google.cloud.bigquery.Schema.of(
                Field.newBuilder("Int_Record", LegacySQLTypeName.RECORD,
                        of("int", LegacySQLTypeName.RECORD, of("int", StandardSQLTypeName.INT64))).setMode(REQUIRED).build());
        Assert.assertEquals(recordSchemaRecord.toString(), SchemaTranslator.translate(recordRecordRecords).toString());
        Assert.assertEquals(SchemaTranslator.translate(recordRecordRecords).getFields().size(), 1);
        Assert.assertEquals(SchemaTranslator.translate(recordRecordRecords).getFields().get(0).getName(), "Int_Record");
        Assert.assertEquals(SchemaTranslator.translate(recordRecordRecords).getFields().get(0).getType(), LegacySQLTypeName.RECORD);
        Assert.assertEquals(SchemaTranslator.translate(recordRecordRecords).getFields().get(0).getMode(), REQUIRED);

    }


    /**
     * Nested Records
     */


    @Test
    public void testNestedRecordsSchema() {
        Schema recordRecord = SchemaBuilder
                .record("user").doc("scribe").namespace("com.wayfair.avro")
                .fields()
                .name("scribe_subscriber_id").doc("Account Number").type().nullable().intType().noDefault()
                .name("scribe_subscriber_name").doc("Customer Name").type(SchemaBuilder
                        .record("UserName")
                        .fields()
                        .name("first_name").doc("First Name").type().nullable().stringType().noDefault()
                        .name("middle_name").doc("Middle Name").type().stringType().noDefault()
                        .name("last_name").doc("Last Name").type().nullable().stringType().noDefault()
                        .endRecord())
                .noDefault()
                .name("scribe_subscriber_location").doc("Customer Location").type().nullable().stringType().noDefault()
                .name("scribe_subscriber_mobile").doc("Customer Mobile Number").type().nullable().stringType().noDefault()
                .name("scribe_subscriber_email").doc("Customer Email").type().nullable().stringType().noDefault()
                .name("scribe_hit_uri").doc("Customer URL & Locale").type().nullable().stringType().noDefault()
                .name("scribe_dummy").doc("Adhoc Data Generation").type().nullable().stringType().noDefault()
                .endRecord();

        com.google.cloud.bigquery.Schema schema = com.google.cloud.bigquery.Schema.of(
                Field.newBuilder("scribe_subscriber_id", StandardSQLTypeName.INT64).setMode(NULLABLE).build(),
                Field.newBuilder("scribe_subscriber_name", StandardSQLTypeName.STRUCT,
                        of("first_name", StandardSQLTypeName.STRING),
                        of("middle_name", StandardSQLTypeName.STRING),
                        of("last_name", StandardSQLTypeName.STRING)).setMode(REQUIRED).build(),
                Field.newBuilder("scribe_subscriber_location", StandardSQLTypeName.STRING).setMode(NULLABLE).build(),
                Field.newBuilder("scribe_subscriber_mobile", StandardSQLTypeName.STRING).setMode(NULLABLE).build(),
                Field.newBuilder("scribe_subscriber_email", StandardSQLTypeName.STRING).setMode(NULLABLE).build(),
                Field.newBuilder("scribe_hit_uri", StandardSQLTypeName.STRING).setMode(NULLABLE).build(),
                Field.newBuilder("scribe_dummy", StandardSQLTypeName.STRING).setMode(NULLABLE).build());

        Assert.assertEquals(schema.toString(), SchemaTranslator.translate(recordRecord).toString());
        Assert.assertEquals(SchemaTranslator.translate(recordRecord).getFields().size(), 7);
        Assert.assertEquals(SchemaTranslator.translate(recordRecord).getFields().get(0).getName(), "scribe_subscriber_id");
        Assert.assertEquals(SchemaTranslator.translate(recordRecord).getFields().get(0).getType(), LegacySQLTypeName.INTEGER);
        Assert.assertEquals(SchemaTranslator.translate(recordRecord).getFields().get(0).getMode(), NULLABLE);

    }


    /**
     * Logical Types
     */


    @Test
    public void testUuidSchema() {
        Schema uuid = LogicalTypes.uuid().addToSchema(Schema.create(Schema.Type.LONG));
        Schema uuidRecord = SchemaBuilder
                .record("uuidTypeSchemaCheck")
                .fields()
                .name("uuid").type().
                        unionOf().
                        nullType().and().
                        type(uuid).
                        endUnion().noDefault()
                .endRecord();
        com.google.cloud.bigquery.Schema uuidSchema = com.google.cloud.bigquery.Schema.of(
                Field.newBuilder("uuid", StandardSQLTypeName.INT64).setMode(NULLABLE).build());
        Assert.assertEquals(uuidSchema, SchemaTranslator.translate(uuidRecord));
        Assert.assertEquals(SchemaTranslator.translate(uuidRecord).getFields().size(), 1);
        Assert.assertEquals(SchemaTranslator.translate(uuidRecord).getFields().get(0).getName(), "uuid");
        Assert.assertEquals(SchemaTranslator.translate(uuidRecord).getFields().get(0).getType(), LegacySQLTypeName.INTEGER);
        Assert.assertEquals(SchemaTranslator.translate(uuidRecord).getFields().get(0).getMode(), NULLABLE);

    }


    @Test
    public void testDateSchema() {
        Schema date = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
        Schema dateRecord = SchemaBuilder
                .record("dateTypeSchemaCheck")
                .fields()
                .name("date").type().
                        unionOf().
                        nullType().and().
                        type(date).
                        endUnion().noDefault()
                .endRecord();
        com.google.cloud.bigquery.Schema dateSchema = com.google.cloud.bigquery.Schema.of(
                Field.newBuilder("date", StandardSQLTypeName.DATE).setMode(NULLABLE).build());
        Assert.assertEquals(dateSchema, SchemaTranslator.translate(dateRecord));
        Assert.assertEquals(SchemaTranslator.translate(dateRecord).getFields().size(), 1);
        Assert.assertEquals(SchemaTranslator.translate(dateRecord).getFields().get(0).getName(), "date");
        Assert.assertEquals(SchemaTranslator.translate(dateRecord).getFields().get(0).getType(), LegacySQLTypeName.DATE);
        Assert.assertEquals(SchemaTranslator.translate(dateRecord).getFields().get(0).getMode(), NULLABLE);

    }


    @Test
    public void testTimeMillisSchema() {
        Schema timeMillis = LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT));
        Schema timeMillisRecord = SchemaBuilder
                .record("timeMillisTypeSchemaCheck")
                .fields()
                .name("timeMillis").type().
                        unionOf().
                        nullType().and().
                        type(timeMillis).
                        endUnion().noDefault()
                .endRecord();
        com.google.cloud.bigquery.Schema timeMillisSchema = com.google.cloud.bigquery.Schema.of(
                Field.newBuilder("timeMillis", StandardSQLTypeName.TIME).setMode(NULLABLE).build());
        Assert.assertEquals(timeMillisSchema, SchemaTranslator.translate(timeMillisRecord));
        Assert.assertEquals(SchemaTranslator.translate(timeMillisRecord).getFields().size(), 1);
        Assert.assertEquals(SchemaTranslator.translate(timeMillisRecord).getFields().get(0).getName(), "timeMillis");
        Assert.assertEquals(SchemaTranslator.translate(timeMillisRecord).getFields().get(0).getType(), LegacySQLTypeName.TIME);
        Assert.assertEquals(SchemaTranslator.translate(timeMillisRecord).getFields().get(0).getMode(), NULLABLE);

    }


    @Test
    public void testTimeMicrosSchema() {
        Schema timeMicros = LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT));
        Schema timeMicrosRecord = SchemaBuilder
                .record("timeMicrosTypeSchemaCheck")
                .fields()
                .name("timeMicros").type().
                        unionOf().
                        nullType().and().
                        type(timeMicros).
                        endUnion().noDefault()
                .endRecord();
        com.google.cloud.bigquery.Schema timeMicrosSchema = com.google.cloud.bigquery.Schema.of(
                Field.newBuilder("timeMicros", StandardSQLTypeName.TIME).setMode(NULLABLE).build());
        Assert.assertEquals(timeMicrosSchema, SchemaTranslator.translate(timeMicrosRecord));
        Assert.assertEquals(SchemaTranslator.translate(timeMicrosRecord).getFields().size(), 1);
        Assert.assertEquals(SchemaTranslator.translate(timeMicrosRecord).getFields().get(0).getName(), "timeMicros");
        Assert.assertEquals(SchemaTranslator.translate(timeMicrosRecord).getFields().get(0).getType(), LegacySQLTypeName.TIME);
        Assert.assertEquals(SchemaTranslator.translate(timeMicrosRecord).getFields().get(0).getMode(), NULLABLE);

    }


    @Test
    public void testTimestampMillisSchema() {
        Schema timestampMillis = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
        Schema timestampMillisRecord = SchemaBuilder
                .record("timestampMillisTypeSchemaCheck")
                .fields()
                .name("timestampMillis").type().
                        unionOf().
                        nullType().and().
                        type(timestampMillis).
                        endUnion().noDefault()
                .endRecord();
        com.google.cloud.bigquery.Schema timestampMillisSchema = com.google.cloud.bigquery.Schema.of(
                Field.newBuilder("timestampMillis", StandardSQLTypeName.TIMESTAMP).setMode(NULLABLE).build());
        Assert.assertEquals(timestampMillisSchema, SchemaTranslator.translate(timestampMillisRecord));
        Assert.assertEquals(SchemaTranslator.translate(timestampMillisRecord).getFields().size(), 1);
        Assert.assertEquals(SchemaTranslator.translate(timestampMillisRecord).getFields().get(0).getName(), "timestampMillis");
        Assert.assertEquals(SchemaTranslator.translate(timestampMillisRecord).getFields().get(0).getType(), LegacySQLTypeName.TIMESTAMP);
        Assert.assertEquals(SchemaTranslator.translate(timestampMillisRecord).getFields().get(0).getMode(), NULLABLE);

    }


    @Test
    public void testTimestampMicrosSchema() {
        Schema timestampMicros = LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
        Schema timestampMicrosRecord = SchemaBuilder
                .record("timestampMicrosTypeSchemaCheck")
                .fields()
                .name("timestampMicros" +
                        "").type().
                        unionOf().
                        nullType().and().
                        type(timestampMicros).
                        endUnion().noDefault()
                .endRecord();
        com.google.cloud.bigquery.Schema timestampMicrosSchema = com.google.cloud.bigquery.Schema.of(
                Field.newBuilder("timestampMicros", StandardSQLTypeName.TIMESTAMP).setMode(NULLABLE).build());
        Assert.assertEquals(timestampMicrosSchema, SchemaTranslator.translate(timestampMicrosRecord));
        Assert.assertEquals(SchemaTranslator.translate(timestampMicrosRecord).getFields().size(), 1);
        Assert.assertEquals(SchemaTranslator.translate(timestampMicrosRecord).getFields().get(0).getName(), "timestampMicros");
        Assert.assertEquals(SchemaTranslator.translate(timestampMicrosRecord).getFields().get(0).getType(), LegacySQLTypeName.TIMESTAMP);
        Assert.assertEquals(SchemaTranslator.translate(timestampMicrosRecord).getFields().get(0).getMode(), NULLABLE);

    }


    @Test
    public void testDecimalSchema() {
        Schema decimal = LogicalTypes.decimal(6).addToSchema(Schema.create(Schema.Type.BYTES));
        Schema decimalRecord = SchemaBuilder
                .record("decimalTypeSchemaCheck")
                .fields()
                .name("decimal").type().
                        unionOf().
                        nullType().and().
                        type(decimal).
                        endUnion().noDefault()
                .endRecord();
        com.google.cloud.bigquery.Schema decimalSchema = com.google.cloud.bigquery.Schema.of(
                Field.newBuilder("decimal", StandardSQLTypeName.NUMERIC).setMode(NULLABLE).build());
        Assert.assertEquals(decimalSchema, SchemaTranslator.translate(decimalRecord));
        Assert.assertEquals(SchemaTranslator.translate(decimalRecord).getFields().size(), 1);
        Assert.assertEquals(SchemaTranslator.translate(decimalRecord).getFields().get(0).getName(), "decimal");
        Assert.assertEquals(SchemaTranslator.translate(decimalRecord).getFields().get(0).getType(), LegacySQLTypeName.NUMERIC);
        Assert.assertEquals(SchemaTranslator.translate(decimalRecord).getFields().get(0).getMode(), NULLABLE);

    }


    @Test
    public void testDecimalScaleSchema() {
        Schema decimalWithScale = LogicalTypes.decimal(6, 6).addToSchema(Schema.create(Schema.Type.BYTES));
        Schema decimalWithScaleRecord = SchemaBuilder
                .record("decimalWithScaleTypeSchemaCheck")
                .fields()
                .name("decimalWithScale").type().
                        unionOf().
                        nullType().and().
                        type(decimalWithScale).
                        endUnion().noDefault()
                .endRecord();
        com.google.cloud.bigquery.Schema decimalWithScaleSchema = com.google.cloud.bigquery.Schema.of(
                Field.newBuilder("decimalWithScale", StandardSQLTypeName.NUMERIC).setMode(NULLABLE).build());
        Assert.assertEquals(decimalWithScaleSchema, SchemaTranslator.translate(decimalWithScaleRecord));
        Assert.assertEquals(SchemaTranslator.translate(decimalWithScaleRecord).getFields().size(), 1);
        Assert.assertEquals(SchemaTranslator.translate(decimalWithScaleRecord).getFields().get(0).getName(), "decimalWithScale");
        Assert.assertEquals(SchemaTranslator.translate(decimalWithScaleRecord).getFields().get(0).getType(), LegacySQLTypeName.NUMERIC);
        Assert.assertEquals(SchemaTranslator.translate(decimalWithScaleRecord).getFields().get(0).getMode(), NULLABLE);

    }

}