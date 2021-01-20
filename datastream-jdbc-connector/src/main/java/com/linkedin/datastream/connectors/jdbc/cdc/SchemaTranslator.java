package com.linkedin.datastream.connectors.jdbc.cdc;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.commons.lang3.StringUtils;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.function.Function;

import static java.sql.Types.*;
import static java.sql.Types.BLOB;

public class SchemaTranslator {

    public static final String AVRO_SCHEMA_RECORD_NAME = "sqlRecord";
    public static final String SOURCE_SQL_DATA_TYPE = "source.sql.datetype";

    public static final int MAX_DIGITS_IN_BIGINT = 19;
    public static final int MAX_DIGITS_IN_INT = 9;

    public static final int DEFAULT_PRECISION_VALUE = 10;
    public static final int DEFAULT_SCALE_VALUE = 0;
    public static final String AVRO_SCHEMA_NAMESPACE = "com.wayfair.brooklin";

    public static Schema fromResultSet(ResultSetMetaData meta) {
        final int nrOfColumns;
        try {
            nrOfColumns = meta.getColumnCount();
            String tableName = AVRO_SCHEMA_RECORD_NAME;
            if (nrOfColumns > 0) {
                String tableNameFromMeta = meta.getTableName(1);
                if (!StringUtils.isBlank(tableNameFromMeta)) {
                    tableName = tableNameFromMeta;
                }
            }

            final SchemaBuilder.FieldAssembler<Schema> builder = SchemaBuilder.record(tableName).namespace(AVRO_SCHEMA_NAMESPACE).fields();

            addSchemaForDataFields("before_", meta, builder);
            addSchemaForDataFields("after_", meta, builder);

            // handle cdc meta columns: commit_lsn, op and seq_no
            final LogicalTypes.Decimal dec24 = LogicalTypes.decimal(24, 0);
            Schema decimal24Schema = dec24.addToSchema(SchemaBuilder.builder().bytesType());

            builder.name("commit_lsn").type(decimal24Schema).noDefault();
            builder.name("seq_no").type(decimal24Schema).noDefault();
            builder.name("op").type().unionOf().nullBuilder().endNull().and().intType().endUnion().noDefault();

            return builder.endRecord();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return null;
    }

    private static void addSchemaForDataFields(String prefix, ResultSetMetaData meta, SchemaBuilder.FieldAssembler<Schema> builder)
            throws SQLException {

        final int nrOfColumns = meta.getColumnCount();

        // handle business columns after index 5
        /*
         * Some missing Avro types - Decimal, Date types. May need some additional work.
         */
        for (int i = 5; i <= nrOfColumns; i++) {
            /*
             *   as per jdbc 4 specs, getColumnLabel will have the alias for the column, if not it will have the column name.
             *  so it may be a better option to check for columnlabel first and if in case it is null is someimplementation,
             *  check for alias. Postgres is the one that has the null column names for calculated fields.
             */
            String nameOrLabel = StringUtils.isNotEmpty(meta.getColumnLabel(i)) ? meta.getColumnLabel(i) : meta.getColumnName(i);
            String columnName = prefix + nameOrLabel;
            String sqlType = null;
            switch (meta.getColumnType(i)) {
                case CHAR:
                case LONGNVARCHAR:
                case LONGVARCHAR:
                case NCHAR:
                case NVARCHAR:
                case VARCHAR:
                case CLOB:
                case NCLOB:
                case OTHER:
                case Types.SQLXML:
                    builder.name(columnName).type().unionOf().nullBuilder().endNull().and().stringType().endUnion().noDefault();
                    break;

                case BIT:
                case BOOLEAN:
                    builder.name(columnName).type().unionOf().nullBuilder().endNull().and().booleanType().endUnion().noDefault();
                    break;

                case INTEGER:
                    if (meta.isSigned(i) || (meta.getPrecision(i) > 0 && meta.getPrecision(i) < MAX_DIGITS_IN_INT)) {
                        builder.name(columnName).type().unionOf().nullBuilder().endNull().and().intType().endUnion().noDefault();
                    } else {
                        builder.name(columnName).type().unionOf().nullBuilder().endNull().and().longType().endUnion().noDefault();
                    }
                    break;

                case SMALLINT:
                case TINYINT:
                    builder.name(columnName).type().unionOf().nullBuilder().endNull().and().intType().endUnion().noDefault();
                    break;

                case BIGINT:
                    // Check the precision of the BIGINT. Some databases allow arbitrary precision (> 19), but Avro won't handle that.
                    // If the precision > 19 (or is negative), use a string for the type, otherwise use a long. The object(s) will be converted
                    // to strings as necessary
                    int precision = meta.getPrecision(i);
                    if (precision < 0 || precision > MAX_DIGITS_IN_BIGINT) {
                        builder.name(columnName).type().unionOf().nullBuilder().endNull().and().stringType().endUnion().noDefault();
                    } else {
                        builder.name(columnName).type().unionOf().nullBuilder().endNull().and().longType().endUnion().noDefault();
                    }
                    break;

                // java.sql.RowId is interface, is seems to be database
                // implementation specific, let's convert to String
                case ROWID:
                    builder.name(columnName).type().unionOf().nullBuilder().endNull().and().stringType().endUnion().noDefault();
                    break;

                case FLOAT:
                case REAL:
                case 100: //Oracle BINARY_FLOAT type
                    builder.name(columnName).type().unionOf().nullBuilder().endNull().and().floatType().endUnion().noDefault();
                    break;

                case DOUBLE:
                case 101: //Oracle BINARY_DOUBLE type
                    builder.name(columnName).type().unionOf().nullBuilder().endNull().and().doubleType().endUnion().noDefault();
                    break;

                // Since Avro 1.8, LogicalType is supported.
                case DECIMAL:
                case NUMERIC:

                    final int decimalPrecision;
                    final int decimalScale;
                    if (meta.getPrecision(i) > 0) {
                        // When database returns a certain precision, we can rely on that.
                        decimalPrecision = meta.getPrecision(i);
                        //For the float data type Oracle return decimalScale < 0 which cause is not expected to org.apache.avro.LogicalTypes
                        //Hence falling back to default scale if decimalScale < 0
                        decimalScale = meta.getScale(i) > 0 ? meta.getScale(i) : DEFAULT_SCALE_VALUE;
                    } else {
                        // If not, use default precision.
                        decimalPrecision = DEFAULT_PRECISION_VALUE;
                        // Oracle returns precision=0, scale=-127 for variable scale value such as ROWNUM or function result.
                        // Specifying 'oracle.jdbc.J2EE13Compliant' SystemProperty makes it to return scale=0 instead.
                        // Queries for example, 'SELECT 1.23 as v from DUAL' can be problematic because it can't be mapped with decimal with scale=0.
                        // Default scale is used to preserve decimals in such case.
                        decimalScale = meta.getScale(i) > 0 ? meta.getScale(i) : DEFAULT_SCALE_VALUE;
                    }
                    final LogicalTypes.Decimal decimal = LogicalTypes.decimal(decimalPrecision, decimalScale);

                    addNullableField(builder, columnName,
                            u -> u.type(decimal.addToSchema(SchemaBuilder.builder().bytesType())));

                    break;

                case DATE:
                    addNullableField(builder, columnName,
                            u -> u.type(LogicalTypes.date().addToSchema(SchemaBuilder.builder().intType())));
                    break;

                case TIME:
                    addNullableField(builder, columnName,
                            u -> u.type(LogicalTypes.timeMillis().addToSchema(SchemaBuilder.builder().intType())));
                    break;

                case -101: // Oracle's TIMESTAMP WITH TIME ZONE
                case -102: // Oracle's TIMESTAMP WITH LOCAL TIME ZONE
                    addNullableField(builder, columnName,
                            u -> u.type(LogicalTypes.timestampMillis().addToSchema(SchemaBuilder.builder().longType())));
                    break;

                case TIMESTAMP:
                case TIMESTAMP_WITH_TIMEZONE:
                case microsoft.sql.Types.SMALLDATETIME: // -150 represents TSQL smalldatetime type
                case microsoft.sql.Types.DATETIME: // -151 represents TSQL datetime type
                case microsoft.sql.Types.DATETIMEOFFSET: //  -155 represents TSQL DATETIMEOFFSET type
                    try {
                        sqlType = meta.getColumnTypeName(i);
                    } catch (SQLException ex) {
                        System.out.println("Column type name not found for value " + meta.getColumnType(i));
                    }
                    final Schema timestampMilliType = LogicalTypes.timestampMillis().addToSchema(SchemaBuilder.builder().longType());
                    if (sqlType != null) {
                        timestampMilliType.addProp(SOURCE_SQL_DATA_TYPE, sqlType);
                    }
                    builder.name(columnName).type().unionOf().nullBuilder().endNull().and().type(timestampMilliType).endUnion().nullDefault();
                    break;

                case BINARY:
                case VARBINARY:
                case LONGVARBINARY:
                case ARRAY:
                case BLOB:
                    builder.name(columnName).type().unionOf().nullBuilder().endNull().and().bytesType().endUnion().noDefault();
                    break;


                default:
                    throw new IllegalArgumentException("createSchema: Unknown SQL type " + meta.getColumnType(i) + " / " + meta.getColumnTypeName(i)
                            + " (table: " + AVRO_SCHEMA_RECORD_NAME + ", column: " + columnName + ") cannot be converted to Avro type");
                    // todo change table name

            }
        }
    }

    private static void addNullableField(
            SchemaBuilder.FieldAssembler<Schema> builder,
            String columnName,
            Function<SchemaBuilder.BaseTypeBuilder<SchemaBuilder.UnionAccumulator<SchemaBuilder.NullDefault<Schema>>>,
                    SchemaBuilder.UnionAccumulator<SchemaBuilder.NullDefault<Schema>>> func
    ) {
        final SchemaBuilder.BaseTypeBuilder<SchemaBuilder.UnionAccumulator<SchemaBuilder.NullDefault<Schema>>>
                and = builder.name(columnName).type().unionOf().nullType().and();
        func.apply(and).endUnion().noDefault();
    }
}
