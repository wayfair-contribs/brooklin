/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.bigquery.schema;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.Validate;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.common.collect.ImmutableList;


/**
 * A simple/basic implementation of a BigQuerySchemaEvolver.
 */
public class SimpleBigquerySchemaEvolver implements BigquerySchemaEvolver {

    @Override
    public Schema evolveSchema(final Schema baseSchema, final Schema newSchema) throws IncompatibleSchemaEvolutionException {
        return Schema.of(mergeFields(baseSchema.getFields(), newSchema.getFields()));
    }

    FieldList mergeFields(final FieldList baseFields, final FieldList newFields) throws IncompatibleSchemaEvolutionException {
        if (baseFields == newFields) {
            return newFields;
        } else {
            final Map<String, Field> newSchemaFieldsByName = newFields.stream()
                    .collect(Collectors.toMap(Field::getName, field -> field));
            final Set<String> existingFieldsNames = baseFields.stream().map(Field::getName).collect(Collectors.toSet());
            final List<Field> evolvedSchemaFields = baseFields.stream()
                    .map(field -> mergeFields(field, newSchemaFieldsByName.get(field.getName())))
                    .collect(Collectors.toList());
            final List<Field> addedFields = newFields.stream()
                    .filter(field -> !existingFieldsNames.contains(field.getName()))
                    .map(field -> mergeFields(null, field)).collect(Collectors.toList());
            return FieldList.of(ImmutableList.<Field>builder().addAll(evolvedSchemaFields).addAll(addedFields).build());
        }
    }

    Field makeNullable(final Field field) {
        final Field nullableField;
        if (field.getMode() == Field.Mode.REPEATED) {
            nullableField = field;
        } else {
            nullableField = field.toBuilder().setMode(Field.Mode.NULLABLE).build();
        }
        return nullableField;
    }

    Field addedField(final Field field) {
        return makeNullable(field);
    }

    Field removedField(final Field field) {
        return makeNullable(field);
    }

    Field mergeFields(final Field baseField, final Field newField) throws IncompatibleSchemaEvolutionException {
        final Field mergedField;
        if (baseField == null && newField == null) {
            throw new IllegalArgumentException("baseField and newField cannot both be null");
        } else if (baseField == null) {
            mergedField = addedField(newField);
        } else if (newField == null) {
            mergedField = removedField(baseField);
        } else if (baseField.equals(newField)) {
            mergedField = newField;
        } else {
            Validate.isTrue(baseField.getName().equalsIgnoreCase(newField.getName()), "baseField and newField names do not match");
            final Field.Builder mergedFieldBuilder = baseField.toBuilder();
            if (StandardSQLTypeName.STRUCT.equals(baseField.getType().getStandardType())) {
                mergedFieldBuilder.setType(mergeFieldTypes(baseField.getType().getStandardType(), newField.getType().getStandardType()),
                        mergeFields(baseField.getSubFields(), newField.getSubFields()));
            } else {
                mergedFieldBuilder.setType(mergeFieldTypes(baseField.getType().getStandardType(), newField.getType().getStandardType()));
            }

            final Field.Mode mergedFieldMode = mergeFieldModes(
                    Optional.ofNullable(baseField.getMode()).orElse(Field.Mode.NULLABLE),
                    Optional.ofNullable(newField.getMode()).orElse(Field.Mode.NULLABLE)
            );
            if (baseField.getMode() != null || mergedFieldMode != Field.Mode.NULLABLE) {
                mergedFieldBuilder.setMode(mergedFieldMode);
            }
            mergedField = mergedFieldBuilder.build();
        }
        return mergedField;
    }

    StandardSQLTypeName mergeFieldTypes(final StandardSQLTypeName baseType, final StandardSQLTypeName newType) throws IncompatibleSchemaEvolutionException {
        Validate.notNull(baseType, "baseType cannot be null");
        Validate.notNull(newType, "newType cannot be null");

        // Support BigQuery type coercion described in https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_rules
        final StandardSQLTypeName mergedType;
        switch (newType) {
            case INT64:
                switch (baseType) {
                    case FLOAT64:
                    case NUMERIC:
                    case INT64:
                        mergedType = baseType;
                        break;
                    default:
                        throw new IncompatibleSchemaEvolutionException(String.format("Cannot coerce type %s into %s", newType, baseType));
                }
                break;
            case NUMERIC:
                switch (baseType) {
                    case FLOAT64:
                    case NUMERIC:
                        mergedType = baseType;
                        break;
                    default:
                        throw new IncompatibleSchemaEvolutionException(String.format("Cannot coerce type %s into %s", newType, baseType));
                }
                break;
            default:
                if (newType == baseType) {
                    mergedType = baseType;
                } else {
                    throw new IncompatibleSchemaEvolutionException(String.format("Cannot coerce type %s into %s", newType, baseType));
                }
                break;
        }
        return mergedType;
    }

    Field.Mode mergeFieldModes(final Field.Mode baseMode, final Field.Mode newMode) {
        Validate.notNull(baseMode, "baseMode cannot be null");
        Validate.notNull(newMode, "newMode cannot be null");
        final Field.Mode mergedMode;
        switch (baseMode) {
            case REPEATED:
                switch (newMode) {
                    case REPEATED:
                    case NULLABLE:
                        mergedMode = Field.Mode.REPEATED;
                        break;
                    default:
                        throw new IncompatibleSchemaEvolutionException(String.format("Cannot change field mode from %s to %s", baseMode, newMode));
                }
                break;
            case REQUIRED:
                switch (newMode) {
                    case REQUIRED:
                        mergedMode = Field.Mode.REQUIRED;
                        break;
                    case NULLABLE:
                        mergedMode = Field.Mode.NULLABLE;
                        break;
                    default:
                        throw new IncompatibleSchemaEvolutionException(String.format("Cannot change field mode from %s to %s", baseMode, newMode));
                }
                break;
            case NULLABLE:
                switch (newMode) {
                    case NULLABLE:
                    case REQUIRED:
                        mergedMode = Field.Mode.NULLABLE;
                        break;
                    default:
                        throw new IncompatibleSchemaEvolutionException(String.format("Cannot change field mode from %s to %s", baseMode, newMode));
                }
                break;
            default:
                throw new IncompatibleSchemaEvolutionException(String.format("Cannot change field mode from %s to %s", baseMode, newMode));
        }
        return mergedMode;
    }

}
