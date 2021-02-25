package com.linkedin.datastream.connectors.jdbc.translator;

import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.connectors.jdbc.JDBCColumn;
import microsoft.sql.DateTimeOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

import static com.linkedin.datastream.connectors.jdbc.translator.TranslatorConstants.*;
import static java.sql.Types.*;
import static java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME;

public class ColumnConverter {
    private static final Logger LOG = LoggerFactory.getLogger(ColumnConverter.class);

    public Object convert(JDBCColumn column) {
        Object value = null;
        if (column == null || column.value() == null) {
            return null;
        }

        if (LOG.isTraceEnabled()) {
            LOG.trace("Converting column: {}", column);
        }

        switch (column.sqlType()) {
            case DATE:
                // value = convertToDate((Date) column.value());
                // todo for testing
                value = 100;
                break;

            case TIME:
                value = convertToTime((Time) column.value());
                break;

            case TIMESTAMP: // 93
            case microsoft.sql.Types.SMALLDATETIME: // -150 represents TSQL smalldatetime type
            case microsoft.sql.Types.DATETIME: // -151 represents TSQL datetime type
                value = convertToTimestamp((Timestamp) column.value());
                break;

            case TIMESTAMP_WITH_TIMEZONE: // 2014
            case microsoft.sql.Types.DATETIMEOFFSET: // -155 represents TSQL Datetimeoffset type
                value = convertToZonedTimestamp((DateTimeOffset) column.value());
                break;

            case BINARY:
            case VARBINARY:
            case ARRAY:
                value = convertToBytes((byte[]) column.value());
                break;

            case DECIMAL_BYTES_TYPE: // Brooklin Type
                value = convertToDecimal((byte[]) column.value());
                break;

            case NUMERIC:
            case DECIMAL:
                value = convertToDecimal((BigDecimal) column.value());
                break;

            case CDC_OP_TYPE:
                value = convertOpToStr( (int) column.value());
                break;

            case TINYINT:
            case SMALLINT:
                // Avro doesn't have type for short
                value = ((Short) column.value()).intValue();
                break;

            default: // for all other types
                value = column.value();
                break;
        }

        return value;
    }

    private Object convertToZonedTimestamp(DateTimeOffset value) {
        return value.getOffsetDateTime().format(ISO_OFFSET_DATE_TIME);
    }

    private Object convertOpToStr(int op) {
        String val = null;
        switch (op) {
            case 1:
                val = "d";
                break;
            case 2:
                val = "i";
                break;
            case 3:
            case 4:
                val = "u";
                break;
            default:
                throw new DatastreamRuntimeException("Invalid operation: " + op);
        }
        return val;
    }

    private Object convertToTimestamp(Timestamp timestamp) {
        // milli second should be enough.
        return timestamp.getTime();
    }

    private Object convertToTime(Time time) {
        // milli second should be enough.
        return (int) time.getTime();
    }

    private Object convertToDate(Date date) {
        return (int) date.getTime();
    }

    private Object convertToDecimal(BigDecimal decimal) {
        return ByteBuffer.wrap(decimal.toBigInteger().toByteArray());
    }

    private Object convertToDecimal(byte[] bytes) {
        return ByteBuffer.wrap(bytes);
    }

    private Object convertToBytes(byte[] bytes) {
        // Kafka Avro Serializer knows ByteBuffer rather than byte[]
        return ByteBuffer.wrap(bytes);
    }
}
