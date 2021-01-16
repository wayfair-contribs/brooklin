/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.bigquery.schema;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Optional;

import static org.testng.Assert.assertEquals;

@Test
public class DynamicBigquerySchemaEvolverTests {

    private static DynamicBigquerySchemaEvolver schemaEvolver = null;

    @BeforeClass
    public void beforeTests() {
        schemaEvolver = new DynamicBigquerySchemaEvolver();
    }


    @Test
    public void testMergeSameFields() {
        final Field baseField = requiredFieldOf("string", StandardSQLTypeName.STRING);
        final Field newField = baseField.toBuilder().build();
        assertEquals(baseField, schemaEvolver.mergeFields(baseField, newField));
    }

    @Test
    public void testMergeSameFieldWithModeChangedToNullable() {
        final Field baseField = requiredFieldOf("string", StandardSQLTypeName.STRING);
        final Field newField = baseField.toBuilder().setMode(Field.Mode.NULLABLE).build();
        assertEquals(schemaEvolver.mergeFields(baseField, newField), newField);
    }

    @Test
    public void testMergeSameFieldsWithModeKeptAsNullable() {
        final Field baseField = Field.newBuilder("string", StandardSQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build();
        final Field newField = requiredFieldOf(baseField.getName(), baseField.getType().getStandardType());
        assertEquals(schemaEvolver.mergeFields(baseField, newField), baseField);
    }

    @DataProvider(name = "compatible type coercions")
    public Object[][] compatibleTypeCoercionsDataProvider() {
        return new Object[][] {
                {StandardSQLTypeName.INT64, StandardSQLTypeName.NUMERIC},
                {StandardSQLTypeName.INT64, StandardSQLTypeName.FLOAT64},
                {StandardSQLTypeName.NUMERIC, StandardSQLTypeName.FLOAT64},
        };
    }

    @Test(dataProvider = "compatible type coercions")
    public void testCompatibleTypeCoercions(final StandardSQLTypeName newType, final StandardSQLTypeName baseType) {
        final Field newField = Field.of("field", newType);
        final Field baseField = Field.of("field", baseType);
        assertEquals(schemaEvolver.mergeFields(baseField, newField), baseField);
    }


    @DataProvider(name = "incompatible type coercions")
    public Object[][] incompatibleTypeCoercionsDataProvider() {
        return new Object[][] {
                {StandardSQLTypeName.INT64, StandardSQLTypeName.STRING},
                {StandardSQLTypeName.FLOAT64, StandardSQLTypeName.INT64},
                {StandardSQLTypeName.FLOAT64, StandardSQLTypeName.NUMERIC},
                {StandardSQLTypeName.FLOAT64, StandardSQLTypeName.STRING},
                {StandardSQLTypeName.NUMERIC, StandardSQLTypeName.INT64},
                {StandardSQLTypeName.NUMERIC, StandardSQLTypeName.STRING},
        };
    }

    @Test(dataProvider = "incompatible type coercions", expectedExceptions = IncompatibleSchemaEvolutionException.class, expectedExceptionsMessageRegExp = "Cannot coerce type .+ into .+")
    public void testIncompatibleTypeCoercions(final StandardSQLTypeName newType, final StandardSQLTypeName baseType) {
        final Field newField = Field.of("field", newType);
        final Field baseField = Field.of("field", baseType);
        schemaEvolver.mergeFields(baseField, newField);
    }

    @DataProvider(name = "compatible mode evolutions")
    public Object[][] compatibleModeEvolutionDataProvider() {
        return new Object[][] {
                {null, null, null},
                {null, Field.Mode.NULLABLE, null},
                {null, Field.Mode.REQUIRED, null},
                {Field.Mode.NULLABLE, Field.Mode.REQUIRED, Field.Mode.NULLABLE},
                {Field.Mode.NULLABLE, null, Field.Mode.NULLABLE},
                {Field.Mode.REQUIRED, Field.Mode.REQUIRED, Field.Mode.REQUIRED},
                {Field.Mode.REQUIRED, Field.Mode.NULLABLE, Field.Mode.NULLABLE},
                {Field.Mode.REQUIRED, null, Field.Mode.NULLABLE},
                {Field.Mode.REPEATED, Field.Mode.REPEATED, Field.Mode.REPEATED},
                {Field.Mode.REPEATED, Field.Mode.NULLABLE, Field.Mode.REPEATED},
                {Field.Mode.REPEATED, null, Field.Mode.REPEATED},
        };
    }

    @Test(dataProvider = "compatible mode evolutions")
    public void compatibleModeEvolutions(final Field.Mode baseMode, final Field.Mode newMode, final Field.Mode expectedMode) {
        final Field.Builder baseFieldBuilder = Field.newBuilder("field", StandardSQLTypeName.STRING);
        Optional.ofNullable(baseMode).ifPresent(baseFieldBuilder::setMode);
        final Field baseField = baseFieldBuilder.build();

        final Field.Builder newFieldBuilder = Field.newBuilder("field", StandardSQLTypeName.STRING);
        Optional.ofNullable(newMode).ifPresent(newFieldBuilder::setMode);
        final Field newField = newFieldBuilder.build();

        assertEquals(schemaEvolver.mergeFields(baseField, newField).getMode(), expectedMode);
    }

    @DataProvider(name = "incompatible mode evolutions")
    public Object[][] incompatibleModeEvolutionDataProvider() {
        return new Object[][] {
                {null, Field.Mode.REPEATED},
                {Field.Mode.NULLABLE, Field.Mode.REPEATED},
                {Field.Mode.REQUIRED, Field.Mode.REPEATED},
        };
    }

    @Test(dataProvider = "incompatible mode evolutions", expectedExceptions = IncompatibleSchemaEvolutionException.class, expectedExceptionsMessageRegExp = "Cannot change field mode from .+ to .+")
    public void incompatibleModeEvolutions(final Field.Mode baseMode, final Field.Mode newMode) {
        final Field.Builder baseFieldBuilder = Field.newBuilder("field", StandardSQLTypeName.STRING);
        Optional.ofNullable(baseMode).ifPresent(baseFieldBuilder::setMode);
        final Field baseField = baseFieldBuilder.build();

        final Field.Builder newFieldBuilder = Field.newBuilder("field", StandardSQLTypeName.STRING);
        Optional.ofNullable(newMode).ifPresent(newFieldBuilder::setMode);
        final Field newField = newFieldBuilder.build();

        schemaEvolver.mergeFields(baseField, newField);
    }

    @Test
    public void testMergeNewField() {
        final Field newField = Field.of("string", StandardSQLTypeName.STRING);
        assertEquals(newField.toBuilder().setMode(Field.Mode.NULLABLE).build(),
                schemaEvolver.mergeFields(null, newField));
    }

    @Test
    public void testRemoveField() {
        final Field baseField = Field.of("string", StandardSQLTypeName.STRING);
        assertEquals(baseField.toBuilder().setMode(Field.Mode.NULLABLE).build(),
                schemaEvolver.mergeFields(baseField, null));
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "baseField and newField cannot both be null")
    public void testNullFieldsException() {
        schemaEvolver.mergeFields((Field)null, null);
    }

    @Test(expectedExceptions = IncompatibleSchemaEvolutionException.class, expectedExceptionsMessageRegExp = "Cannot coerce type .+ into .+")
    public void testFieldTypeMissmatchException() {
        final Field baseField = Field.of("string", StandardSQLTypeName.STRING);
        final Field newField = baseField.toBuilder().setType(StandardSQLTypeName.INT64).build();
        assertEquals(baseField, schemaEvolver.mergeFields(baseField, newField));
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ".+ and .+ names do not match")
    public void testFieldNameMissmatchException() {
        final Field baseField = Field.of("string1", StandardSQLTypeName.STRING);
        final Field newField = baseField.toBuilder().setName("string2").build();
        schemaEvolver.mergeFields(baseField, newField);
    }

    @Test
    public void testAddNewSchemaField() {
        final Schema baseSchema = Schema.of(
                Field.newBuilder("string", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build(),
                Field.newBuilder("int", StandardSQLTypeName.INT64).setMode(Field.Mode.REQUIRED).build()
                );
        final Schema newSchema = Schema.of(
                Field.newBuilder("string", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build(),
                Field.newBuilder("int", StandardSQLTypeName.INT64).setMode(Field.Mode.REQUIRED).build(),
                Field.newBuilder("new_string", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build()
        );
        final Schema evolvedSchema = schemaEvolver.evolveSchema(baseSchema, newSchema);
        assertEquals(Schema.of(
                Field.newBuilder("string", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build(),
                Field.newBuilder("int", StandardSQLTypeName.INT64).setMode(Field.Mode.REQUIRED).build(),
                Field.newBuilder("new_string", StandardSQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
        ), evolvedSchema);
    }

    @Test
    public void testAddNewSchemaFieldWithRequiredNestedStructure() {
        final Schema baseSchema = Schema.of(
                Field.newBuilder("string", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build(),
                Field.newBuilder("int", StandardSQLTypeName.INT64).setMode(Field.Mode.REQUIRED).build()
        );
        final Schema newSchema = Schema.of(
                Field.newBuilder("string", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build(),
                Field.newBuilder("int", StandardSQLTypeName.INT64).setMode(Field.Mode.REQUIRED).build(),
                Field.newBuilder("new_nested", StandardSQLTypeName.STRUCT,
                        Field.newBuilder("nested_string", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build()
                ).setMode(Field.Mode.REQUIRED).build()
        );
        final Schema evolvedSchema = schemaEvolver.evolveSchema(baseSchema, newSchema);
        assertEquals(Schema.of(
                Field.newBuilder("string", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build(),
                Field.newBuilder("int", StandardSQLTypeName.INT64).setMode(Field.Mode.REQUIRED).build(),
                Field.newBuilder("new_nested", StandardSQLTypeName.STRUCT,
                        Field.newBuilder("nested_string", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build()
                ).setMode(Field.Mode.NULLABLE).build()
        ), evolvedSchema);
    }

    @Test
    public void testAddNewNestedSchemaField() {
        final Schema baseSchema = Schema.of(
                Field.newBuilder("string", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build(),
                Field.newBuilder("int", StandardSQLTypeName.INT64).setMode(Field.Mode.REQUIRED).build(),
                Field.newBuilder("nested", StandardSQLTypeName.STRUCT,
                        Field.newBuilder("nested_string", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build()
                ).setMode(Field.Mode.REQUIRED).build()
        );
        final Schema newSchema = Schema.of(
                Field.newBuilder("string", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build(),
                Field.newBuilder("int", StandardSQLTypeName.INT64).setMode(Field.Mode.REQUIRED).build(),
                Field.newBuilder("nested", StandardSQLTypeName.STRUCT,
                        Field.newBuilder("nested_string", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build(),
                        Field.newBuilder("new_nested_int", StandardSQLTypeName.INT64).setMode(Field.Mode.REQUIRED).build()
                ).setMode(Field.Mode.REQUIRED).build()
        );
        final Schema evolvedSchema = schemaEvolver.evolveSchema(baseSchema, newSchema);
        assertEquals(evolvedSchema, Schema.of(
                Field.newBuilder("string", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build(),
                Field.newBuilder("int", StandardSQLTypeName.INT64).setMode(Field.Mode.REQUIRED).build(),
                Field.newBuilder("nested", StandardSQLTypeName.STRUCT,
                        Field.newBuilder("nested_string", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build(),
                        Field.newBuilder("new_nested_int", StandardSQLTypeName.INT64).setMode(Field.Mode.NULLABLE).build()
                ).setMode(Field.Mode.REQUIRED).build()
        ));
    }

    @Test
    public void testRemoveSchemaField() {
        final Schema baseSchema = Schema.of(
                Field.newBuilder("string", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build(),
                Field.newBuilder("int", StandardSQLTypeName.INT64).setMode(Field.Mode.REQUIRED).build(),
                Field.newBuilder("old_string", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build()
        );
        final Schema newSchema = Schema.of(
                Field.newBuilder("string", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build(),
                Field.newBuilder("int", StandardSQLTypeName.INT64).setMode(Field.Mode.REQUIRED).build()
                );
        final Schema evolvedSchema = schemaEvolver.evolveSchema(baseSchema, newSchema);
        assertEquals(evolvedSchema, Schema.of(
                Field.newBuilder("string", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build(),
                Field.newBuilder("int", StandardSQLTypeName.INT64).setMode(Field.Mode.REQUIRED).build(),
                Field.newBuilder("old_string", StandardSQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
        ));
    }

    @Test
    public void testRemoveMiddleSchemaField() {
        final Schema baseSchema = Schema.of(
                Field.newBuilder("string", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build(),
                Field.newBuilder("int", StandardSQLTypeName.INT64).setMode(Field.Mode.REQUIRED).build(),
                Field.newBuilder("old_string", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build()
        );
        final Schema newSchema = Schema.of(
                Field.newBuilder("string", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build(),
                Field.newBuilder("old_string", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build()
        );
        final Schema evolvedSchema = schemaEvolver.evolveSchema(baseSchema, newSchema);
        assertEquals(evolvedSchema, Schema.of(
                Field.newBuilder("string", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build(),
                Field.newBuilder("int", StandardSQLTypeName.INT64).setMode(Field.Mode.NULLABLE).build(),
                Field.newBuilder("old_string", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build()
        ));
    }

    @Test
    public void testRemoveSchemaFieldWithRequiredNestedStructure() {
        final Schema baseSchema = Schema.of(
                Field.newBuilder("string", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build(),
                Field.newBuilder("int", StandardSQLTypeName.INT64).setMode(Field.Mode.REQUIRED).build(),
                Field.newBuilder("nested", StandardSQLTypeName.STRUCT,
                        Field.newBuilder("nested_string", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build(),
                        Field.newBuilder("nested_int", StandardSQLTypeName.INT64).setMode(Field.Mode.REQUIRED).build()
                ).setMode(Field.Mode.REQUIRED).build()
        );
        final Schema newSchema = Schema.of(
                Field.newBuilder("string", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build(),
                Field.newBuilder("int", StandardSQLTypeName.INT64).setMode(Field.Mode.REQUIRED).build()
        );
        final Schema evolvedSchema = schemaEvolver.evolveSchema(baseSchema, newSchema);
        assertEquals(evolvedSchema, Schema.of(
                Field.newBuilder("string", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build(),
                Field.newBuilder("int", StandardSQLTypeName.INT64).setMode(Field.Mode.REQUIRED).build(),
                Field.newBuilder("nested", StandardSQLTypeName.STRUCT,
                        Field.newBuilder("nested_string", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build(),
                        Field.newBuilder("nested_int", StandardSQLTypeName.INT64).setMode(Field.Mode.REQUIRED).build()
                ).setMode(Field.Mode.NULLABLE).build()
        ));
    }

    @Test
    public void testRemoveNestedSchemaField() {
        final Schema baseSchema = Schema.of(
                Field.newBuilder("string", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build(),
                Field.newBuilder("int", StandardSQLTypeName.INT64).setMode(Field.Mode.REQUIRED).build(),
                Field.newBuilder("nested", StandardSQLTypeName.STRUCT,
                        Field.newBuilder("nested_string", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build(),
                        Field.newBuilder("nested_int", StandardSQLTypeName.INT64).setMode(Field.Mode.REQUIRED).build()
                ).setMode(Field.Mode.REQUIRED).build()
        );
        final Schema newSchema = Schema.of(
                Field.newBuilder("string", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build(),
                Field.newBuilder("int", StandardSQLTypeName.INT64).setMode(Field.Mode.REQUIRED).build(),
                Field.newBuilder("nested", StandardSQLTypeName.STRUCT,
                        Field.newBuilder("nested_int", StandardSQLTypeName.INT64).setMode(Field.Mode.REQUIRED).build()
                ).setMode(Field.Mode.REQUIRED).build()
        );
        final Schema evolvedSchema = schemaEvolver.evolveSchema(baseSchema, newSchema);
        assertEquals(evolvedSchema, Schema.of(
                Field.newBuilder("string", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build(),
                Field.newBuilder("int", StandardSQLTypeName.INT64).setMode(Field.Mode.REQUIRED).build(),
                Field.newBuilder("nested", StandardSQLTypeName.STRUCT,
                        Field.newBuilder("nested_string", StandardSQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build(),
                        Field.newBuilder("nested_int", StandardSQLTypeName.INT64).setMode(Field.Mode.REQUIRED).build()
                ).setMode(Field.Mode.REQUIRED).build()
        ));
    }

    @Test
    public void testSchemaFieldModeChangedToNullable() {
        final Schema baseSchema = Schema.of(
                Field.newBuilder("string", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build(),
                Field.newBuilder("int", StandardSQLTypeName.INT64).setMode(Field.Mode.REQUIRED).build()
                );
        final Schema newSchema = Schema.of(
                Field.newBuilder("string", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build(),
                Field.newBuilder("int", StandardSQLTypeName.INT64).setMode(Field.Mode.NULLABLE).build()
                );
        final Schema evolvedSchema = schemaEvolver.evolveSchema(baseSchema, newSchema);
        assertEquals(evolvedSchema, newSchema);
    }

    @Test
    public void testSchemaFieldModeChangedToRequiredIsIgnored() {
        final Schema baseSchema = Schema.of(
                Field.of("string", StandardSQLTypeName.STRING),
                Field.newBuilder("int", StandardSQLTypeName.INT64).setMode(Field.Mode.NULLABLE).build()
                );
        final Schema newSchema = Schema.of(
                Field.of("string", StandardSQLTypeName.STRING),
                Field.of("int", StandardSQLTypeName.INT64)
                );
        final Schema evolvedSchema = schemaEvolver.evolveSchema(baseSchema, newSchema);
        assertEquals(baseSchema, evolvedSchema);
    }

    @Test
    public void testSchemaFieldOrderingNotChangedOnAdd() {
        final Schema baseSchema = Schema.of(
                Field.of("int", StandardSQLTypeName.INT64),
                Field.of("string", StandardSQLTypeName.STRING)
        );
        final Schema newSchema = Schema.of(
                Field.of("string", StandardSQLTypeName.STRING),
                Field.of("newInt", StandardSQLTypeName.INT64),
                Field.of("int", StandardSQLTypeName.INT64)
        );
        final Schema evolvedSchema = schemaEvolver.evolveSchema(baseSchema, newSchema);
        assertEquals(evolvedSchema, Schema.of(
                Field.of("int", StandardSQLTypeName.INT64),
                Field.of("string", StandardSQLTypeName.STRING),
                Field.newBuilder("newInt", StandardSQLTypeName.INT64).setMode(Field.Mode.NULLABLE).build()
        ));
    }

    @Test
    public void testSchemaFieldOrderingNotChangedOnAddNested() {
        final Schema baseSchema = Schema.of(
                Field.of("int", StandardSQLTypeName.INT64),
                Field.of("nested", StandardSQLTypeName.STRUCT,
                        Field.of("nested_int", StandardSQLTypeName.INT64),
                        Field.of("nested_string", StandardSQLTypeName.STRING)
                ),
                Field.of("string", StandardSQLTypeName.STRING)
        );
        final Schema newSchema = Schema.of(
                Field.of("string", StandardSQLTypeName.STRING),
                Field.of("newInt", StandardSQLTypeName.INT64),
                Field.of("nested", StandardSQLTypeName.STRUCT,
                        Field.of("nested_int", StandardSQLTypeName.INT64),
                        Field.of("new_nested_string", StandardSQLTypeName.STRING),
                        Field.of("nested_string", StandardSQLTypeName.STRING)
                ),
                Field.of("int", StandardSQLTypeName.INT64)
        );
        final Schema evolvedSchema = schemaEvolver.evolveSchema(baseSchema, newSchema);
        assertEquals(evolvedSchema, Schema.of(
                Field.of("int", StandardSQLTypeName.INT64),
                Field.of("nested", StandardSQLTypeName.STRUCT,
                        Field.of("nested_int", StandardSQLTypeName.INT64),
                        Field.of("nested_string", StandardSQLTypeName.STRING),
                        Field.newBuilder("new_nested_string", StandardSQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
                ),
                Field.of("string", StandardSQLTypeName.STRING),
                Field.newBuilder("newInt", StandardSQLTypeName.INT64).setMode(Field.Mode.NULLABLE).build()
        ));
    }

    @Test
    public void testSchemaFieldOrderingNotChangedOnRemove() {
        final Schema baseSchema = Schema.of(
                Field.newBuilder("int", StandardSQLTypeName.INT64).setMode(Field.Mode.REQUIRED).build(),
                Field.newBuilder("old_string", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build(),
                Field.newBuilder("string", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build()
        );
        final Schema newSchema = Schema.of(
                Field.newBuilder("string", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build(),
                Field.newBuilder("int", StandardSQLTypeName.INT64).setMode(Field.Mode.REQUIRED).build()
        );
        final Schema evolvedSchema = schemaEvolver.evolveSchema(baseSchema, newSchema);
        assertEquals(evolvedSchema, Schema.of(
                Field.newBuilder("int", StandardSQLTypeName.INT64).setMode(Field.Mode.REQUIRED).build(),
                Field.newBuilder("old_string", StandardSQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build(),
                Field.newBuilder("string", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build()
        ));
    }

    @Test
    public void testSchemaFieldOrderingNotChangedOnRemoveNested() {
        final Schema baseSchema = Schema.of(
                Field.newBuilder("int", StandardSQLTypeName.INT64).setMode(Field.Mode.REQUIRED).build(),
                Field.newBuilder("nested", StandardSQLTypeName.STRUCT,
                        Field.newBuilder("nested_string", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build(),
                        Field.newBuilder("nested_int", StandardSQLTypeName.INT64).setMode(Field.Mode.REQUIRED).build(),
                        Field.newBuilder("old_nested_string", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build()
                ).setMode(Field.Mode.REQUIRED).build(),
                Field.newBuilder("string", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build()
        );
        final Schema newSchema = Schema.of(
                Field.newBuilder("nested", StandardSQLTypeName.STRUCT,
                        Field.newBuilder("nested_int", StandardSQLTypeName.INT64).setMode(Field.Mode.REQUIRED).build(),
                        Field.newBuilder("nested_string", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build()
                ).setMode(Field.Mode.REQUIRED).build(),
                Field.newBuilder("string", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build(),
                Field.newBuilder("int", StandardSQLTypeName.INT64).setMode(Field.Mode.REQUIRED).build()
        );
        final Schema evolvedSchema = schemaEvolver.evolveSchema(baseSchema, newSchema);
        assertEquals(evolvedSchema, Schema.of(
                Field.newBuilder("int", StandardSQLTypeName.INT64).setMode(Field.Mode.REQUIRED).build(),
                Field.newBuilder("nested", StandardSQLTypeName.STRUCT,
                        Field.newBuilder("nested_string", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build(),
                        Field.newBuilder("nested_int", StandardSQLTypeName.INT64).setMode(Field.Mode.REQUIRED).build(),
                        Field.newBuilder("old_nested_string", StandardSQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
                ).setMode(Field.Mode.REQUIRED).build(),
                Field.newBuilder("string", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build()
        ));
    }


    private static Field requiredFieldOf(final String name, final StandardSQLTypeName type) {
        return Field.newBuilder(name, type).setMode(Field.Mode.REQUIRED).build();
    }
}
