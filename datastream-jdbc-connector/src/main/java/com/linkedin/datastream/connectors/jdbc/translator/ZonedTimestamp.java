package com.linkedin.datastream.connectors.jdbc.translator;

import org.apache.avro.LogicalType;
import org.apache.avro.Schema;

public class ZonedTimestamp extends LogicalType {
    public ZonedTimestamp(String logicalTypeName) {
        super(logicalTypeName);
    }

    @Override
    public Schema addToSchema(Schema schema) {
        Schema newSchema = super.addToSchema(schema);
        // Decoder will use this property to know this is zoned timestamp
        newSchema.addProp(CustomLogicalTypes.LOGICAL_TYPE_KEY, CustomLogicalTypes.ZONED_TIMESTAMP_NAME);
        return newSchema;
    }

    @Override
    public void validate(Schema schema) {
        super.validate(schema);
    }
}