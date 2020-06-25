/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.bigquery.translator;

import java.util.Arrays;
import java.util.List;

import org.apache.avro.Schema;

class LogicalTypeIdentifier {

    private static final String DEBEZIUM_LOGICAL_TYPE_PROPERTY = "connect.name";

    private static final String DEBEZIUM_DATE_NAME = "io.debezium.time.Date";

    private static final String DEBEZIUM_TIME_NAME = "io.debezium.time.Time";
    private static final String DEBEZIUM_MICROTIME_NAME = "io.debezium.time.MicroTime";
    private static final String DEBEZIUM_NANOTIME_NAME = "io.debezium.time.NanoTime";
    private static final String DEBEZIUM_ZONEDTIME_NAME = "io.debezium.time.ZonedTime";

    private static final String DEBEZIUM_TIMESTAMP_NAME = "io.debezium.time.Timestamp";
    private static final String DEBEZIUM_MICROTIMESTAMP_NAME = "io.debezium.time.MicroTimestamp";
    private static final String DEBEZIUM_NANOTIMESTAMP_NAME = "io.debezium.time.NanoTimestamp";
    private static final String DEBEZIUM_ZONEDTIMESTAMP_NAME = "io.debezium.time.ZonedTimestamp";

    private static final List<String> DEBEZIUM_TIME_TYPES = Arrays.asList(
            DEBEZIUM_TIME_NAME,
            DEBEZIUM_MICROTIME_NAME,
            DEBEZIUM_NANOTIME_NAME,
            DEBEZIUM_ZONEDTIME_NAME);

    private static final List<String> DEBEZIUM_TIMESTAMP_TYPES = Arrays.asList(
            DEBEZIUM_TIMESTAMP_NAME,
            DEBEZIUM_MICROTIMESTAMP_NAME,
            DEBEZIUM_NANOTIMESTAMP_NAME,
            DEBEZIUM_ZONEDTIMESTAMP_NAME);

    private static final String AVRO_DATE_NAME = "date";
    private static final String AVRO_TIMEMILLIS_NAME = "time-millis";
    private static final String AVRO_TIMEMICROS_NAME = "time-micros";
    private static final String AVRO_TIMESTAMPMILLIS_NAME = "timestamp-millis";
    private static final String AVRO_TIMESTAMPMICROS_NAME = "timestamp-micros";
    private static final String AVRO_DECIMAL_NAME = "decimal";

    private static final List<String> AVRO_TIME_TYPES = Arrays.asList(AVRO_TIMEMILLIS_NAME, AVRO_TIMEMICROS_NAME);
    private static final List<String> AVRO_TIMESTAMP_TYPES = Arrays.asList(AVRO_TIMESTAMPMILLIS_NAME, AVRO_TIMESTAMPMICROS_NAME);

    static boolean isDateType(Schema avroSchema) {
        return (avroSchema.getLogicalType() != null && avroSchema.getLogicalType().getName().equals(AVRO_DATE_NAME))
                || (avroSchema.getProp(DEBEZIUM_LOGICAL_TYPE_PROPERTY) != null
                    && avroSchema.getProp(DEBEZIUM_LOGICAL_TYPE_PROPERTY).equals(DEBEZIUM_DATE_NAME));
    }

    static boolean isTimeType(Schema avroSchema) {
        return (avroSchema.getLogicalType() != null && AVRO_TIME_TYPES.contains(avroSchema.getLogicalType().getName()))
                || (avroSchema.getProp(DEBEZIUM_LOGICAL_TYPE_PROPERTY) != null &&
                    DEBEZIUM_TIME_TYPES.contains(avroSchema.getProp(DEBEZIUM_LOGICAL_TYPE_PROPERTY)));
    }

    static boolean isTimestampType(Schema avroSchema) {
        return (avroSchema.getLogicalType() != null && AVRO_TIMESTAMP_TYPES.contains(avroSchema.getLogicalType().getName()))
                || (avroSchema.getProp(DEBEZIUM_LOGICAL_TYPE_PROPERTY) != null &&
                    DEBEZIUM_TIMESTAMP_TYPES.contains(avroSchema.getProp(DEBEZIUM_LOGICAL_TYPE_PROPERTY)));
    }

    static boolean isDecimalType(Schema avroSchema) {
        return avroSchema.getLogicalType() != null && avroSchema.getLogicalType().getName().equals(AVRO_DECIMAL_NAME);
    }

    static boolean isMilliTime(Schema avroSchema) {
        return (avroSchema.getLogicalType() != null && avroSchema.getLogicalType().equals(AVRO_TIMEMILLIS_NAME))
                || (avroSchema.getProp(DEBEZIUM_LOGICAL_TYPE_PROPERTY) != null &&
                avroSchema.getProp(DEBEZIUM_LOGICAL_TYPE_PROPERTY).equals(DEBEZIUM_TIME_NAME));
    }

    static boolean isMicroTime(Schema avroSchema) {
        return (avroSchema.getLogicalType() != null && avroSchema.getLogicalType().equals(AVRO_TIMEMICROS_NAME))
                || (avroSchema.getProp(DEBEZIUM_LOGICAL_TYPE_PROPERTY) != null &&
                avroSchema.getProp(DEBEZIUM_LOGICAL_TYPE_PROPERTY).equals(DEBEZIUM_MICROTIME_NAME));
    }

    static boolean isNanoTime(Schema avroSchema) {
        return (avroSchema.getProp(DEBEZIUM_LOGICAL_TYPE_PROPERTY) != null &&
                avroSchema.getProp(DEBEZIUM_LOGICAL_TYPE_PROPERTY).equals(DEBEZIUM_NANOTIME_NAME));
    }

    static boolean isZonedTime(Schema avroSchema) {
        return (avroSchema.getProp(DEBEZIUM_LOGICAL_TYPE_PROPERTY) != null &&
                avroSchema.getProp(DEBEZIUM_LOGICAL_TYPE_PROPERTY).equals(DEBEZIUM_ZONEDTIME_NAME));
    }

    static boolean isMilliTimestamp(Schema avroSchema) {
        return (avroSchema.getLogicalType() != null && avroSchema.getLogicalType().equals(AVRO_TIMESTAMPMILLIS_NAME))
                || (avroSchema.getProp(DEBEZIUM_LOGICAL_TYPE_PROPERTY) != null &&
                avroSchema.getProp(DEBEZIUM_LOGICAL_TYPE_PROPERTY).equals(DEBEZIUM_TIMESTAMP_NAME));
    }

    static boolean isMicroTimestamp(Schema avroSchema) {
        return (avroSchema.getLogicalType() != null && avroSchema.getLogicalType().equals(AVRO_TIMESTAMPMICROS_NAME))
                || (avroSchema.getProp(DEBEZIUM_LOGICAL_TYPE_PROPERTY) != null &&
                avroSchema.getProp(DEBEZIUM_LOGICAL_TYPE_PROPERTY).equals(DEBEZIUM_MICROTIMESTAMP_NAME));
    }

    static boolean isNanoTimestamp(Schema avroSchema) {
        return (avroSchema.getProp(DEBEZIUM_LOGICAL_TYPE_PROPERTY) != null &&
                avroSchema.getProp(DEBEZIUM_LOGICAL_TYPE_PROPERTY).equals(DEBEZIUM_NANOTIMESTAMP_NAME));
    }

    static boolean isZonedTimestamp(Schema avroSchema) {
        return (avroSchema.getProp(DEBEZIUM_LOGICAL_TYPE_PROPERTY) != null &&
                avroSchema.getProp(DEBEZIUM_LOGICAL_TYPE_PROPERTY).equals(DEBEZIUM_ZONEDTIMESTAMP_NAME));
    }

    static boolean isLSN(String name) {
        return name.equals("change_lsn") || name.equals("commit_lsn");
    }
}
