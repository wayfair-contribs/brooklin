package com.linkedin.datastream.bigquery.schema;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
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
        return field.toBuilder().setMode(Field.Mode.NULLABLE).build();
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
            if (!baseField.getName().equals(newField.getName())) {
                throw new IncompatibleSchemaEvolutionException(String.format("field name missmatch: %s != %s", baseField.getName(), newField.getName()));
            }
            if (!baseField.getType().equals(newField.getType())) {
                throw new IncompatibleSchemaEvolutionException(String.format("field type missmatch: %s != %s", baseField.getType(), newField.getType()));
            }
            final FieldList mergedSubFields;
            if (baseField.getType().equals(LegacySQLTypeName.RECORD)) {
                mergedSubFields = mergeFields(baseField.getSubFields(), newField.getSubFields());
            } else {
                mergedSubFields = null;
            }
            if (baseField.getMode() == Field.Mode.REQUIRED && newField.getMode() == Field.Mode.REQUIRED) {
                mergedField = newField.toBuilder().setType(newField.getType(), mergedSubFields).build();
            } else {
                mergedField = newField.toBuilder().setType(newField.getType(), mergedSubFields).setMode(Field.Mode.NULLABLE).build();
            }
        }
        return mergedField;
    }

}
