package com.linkedin.datastream.connectors.jdbc.translator;

import com.linkedin.datastream.connectors.jdbc.JDBCColumn;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import static com.linkedin.datastream.connectors.jdbc.translator.TranslatorConstants.*;
import static java.sql.Types.*;
import static java.sql.Types.BLOB;

public class AvroFieldSchemaBuilder {

    public void build(JDBCColumn column, SchemaBuilder.FieldAssembler<Schema> fieldAssembler) {

        Schema fieldSchema = fieldSchema(column);
        Schema unioned = unionIfNullable(fieldSchema, column.optional());
        fieldAssembler.name(column.name()).type(unioned).noDefault();
    }

    private Schema unionIfNullable(Schema fieldSchema, boolean optional) {
        return optional ?
                SchemaBuilder.builder().unionOf().nullType().and().type(fieldSchema).endUnion() :
                fieldSchema;
    }

    private Schema fieldSchema(JDBCColumn column) {

        switch (column.sqlType()) {
            case CHAR:
            case LONGNVARCHAR:
            case LONGVARCHAR:
            case NCHAR:
            case NVARCHAR:
            case VARCHAR:
            case CLOB:
            case NCLOB:
            case OTHER:
            case SQLXML:
            case CDC_OP_TYPE: // Custom type
                return SchemaBuilder.builder().stringType();

            case BINARY:
            case VARBINARY:
            case LONGVARBINARY:
            case ARRAY:
            case BLOB:
                return SchemaBuilder.builder().bytesType();

            case BIT:
            case BOOLEAN:
                return SchemaBuilder.builder().booleanType();

            case INTEGER:
            case SMALLINT:
            case TINYINT:
                return SchemaBuilder.builder().intType();

            case BIGINT:
                return SchemaBuilder.builder().longType();

            case REAL:
                return SchemaBuilder.builder().floatType();

            case FLOAT:
                if (column.scale() <= 7) {
                    return SchemaBuilder.builder().floatType();
                } else {
                    return SchemaBuilder.builder().doubleType();
                }

            case DOUBLE:
                return SchemaBuilder.builder().doubleType();

            // Since Avro 1.8, LogicalType is supported.
            case DECIMAL:
            case NUMERIC:
            case DECIMAL_BYTES_TYPE:
                return LogicalTypes.decimal(column.precision(), column.scale())
                        .addToSchema(SchemaBuilder.builder().bytesType());

            case DATE:
                return LogicalTypes.date().addToSchema(SchemaBuilder.builder().intType());

            case TIME:
                return LogicalTypes.timeMillis().addToSchema(SchemaBuilder.builder().intType());

            case TIMESTAMP:
            case microsoft.sql.Types.SMALLDATETIME: // -150 represents TSQL smalldatetime type
            case microsoft.sql.Types.DATETIME: // -151 represents TSQL datetime type
                return LogicalTypes.timestampMillis().addToSchema(SchemaBuilder.builder().longType());

            case TIMESTAMP_WITH_TIMEZONE: // todo: where it comes from and what java class should be?
            case microsoft.sql.Types.DATETIMEOFFSET: //  -155 represents TSQL DATETIMEOFFSET type
                return CustomLogicalTypes.zonedTimestamp().addToSchema(SchemaBuilder.builder().stringType());

            default:
                throw new IllegalArgumentException("createSchema: Unknown SQL type " + column.sqlType() + " / " + column.name()
                        + " (table: " + AVRO_SCHEMA_RECORD_NAME + ", column: " + column.name() + ") cannot be converted to Avro type");
                // todo change table name
        }
    }
}
