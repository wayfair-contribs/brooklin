package com.linkedin.datastream.connectors.jdbc.translator;

public class CustomLogicalTypes {
    public final static String ZONED_TIMESTAMP_NAME = "zoned_timestamp";
    public final static String LOGICAL_TYPE_KEY = "logical_type_key";

    public static ZonedTimestamp zonedTimestamp() {
        return new ZonedTimestamp(ZONED_TIMESTAMP_NAME);
    }
}
