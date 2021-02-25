package com.linkedin.datastream.connectors.jdbc.translator;

public class TranslatorConstants {
    public static final String AVRO_SCHEMA_RECORD_NAME = "sqlRecord";
    public static final String SOURCE_SQL_DATA_TYPE = "source.sql.datetype";

    public static final int MAX_DIGITS_IN_BIGINT = 19;
    public static final int MAX_DIGITS_IN_INT = 9;

    public static final int DEFAULT_PRECISION_VALUE = 10;
    public static final int DEFAULT_SCALE_VALUE = 0;
    public static final String AVRO_SCHEMA_NAMESPACE = "com.wayfair.brooklin";

    public static final String AVRO_FIELD_COMMIT_LSN = "commit_lsn";
    public static final String AVRO_FIELD_CHANGE_LSN = "change_lsn";
    public static final String AVRO_FIELD_EVENT_SERIAL_NO = "event_serial_no";
    public static final String AVRO_FIELD_TS_MS = "ts_ms";
    public static final String AVRO_FIELD_OP = "op";

    // used by CDC
    public static final int DECIMAL_BYTES_TYPE = 1000;
    public static final int CDC_OP_TYPE = 1001;
}
