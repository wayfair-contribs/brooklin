/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.bigquery.translator;

import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.TimeZone;

import org.apache.avro.Schema;

import com.google.api.client.util.DateTime;

class LogicalTypeTranslator {

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
    private static final SimpleDateFormat TIME_FORMAT = new SimpleDateFormat("HH:mm:ss");

    static String translateDateType(long daysSinceEpoch) {
        return DATE_FORMAT.format(new Date(daysSinceEpoch * 86400));
    }

    static DateTime translateTimestampType(long instant, Schema avroSchema) {
        if (LogicalTypeIdentifier.isMilliTimestamp(avroSchema)) {
            return new DateTime(new Date(instant));
        } else if (LogicalTypeIdentifier.isMicroTimestamp(avroSchema)) {
            return new DateTime(new Date(instant / 1000));
        } else if (LogicalTypeIdentifier.isNanoTimestamp(avroSchema)) {
            return new DateTime(new Date(instant / 1000000));
        }
        return null;
    }

    static DateTime translateTimestampType(String instant, Schema avroSchema) {
        if (LogicalTypeIdentifier.isZonedTimestamp(avroSchema)) {
            return new DateTime(Date.from(OffsetDateTime.parse(instant, DateTimeFormatter.ISO_OFFSET_DATE_TIME).toInstant()));
        }
        return null;
    }

    static String translateTimeType(long instant, Schema avroSchema) {
        TIME_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
        if (LogicalTypeIdentifier.isMilliTime(avroSchema)) {
            return TIME_FORMAT.format(new Date(instant)) + "." + String.format("%06d", (instant % 1000) * 1000);
        } else if (LogicalTypeIdentifier.isMicroTime(avroSchema)) {
            return TIME_FORMAT.format(new Date(instant / 1000)) + "." + String.format("%06d", instant % 1000000);
        } else if (LogicalTypeIdentifier.isNanoTime(avroSchema)) {
            return TIME_FORMAT.format(new Date(instant / 1000000)) + "." + String.format("%06d", (instant % 1000000000) / 1000);
        }
        return null;
    }

    static String translateTimeType(String instant, Schema avroSchema) {
        TIME_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
        if (LogicalTypeIdentifier.isZonedTime(avroSchema)) {
            return TIME_FORMAT.format(Date.from(OffsetDateTime.parse(instant, DateTimeFormatter.ISO_OFFSET_TIME).toInstant()));
        }
        return null;
    }

    static byte[] translateLSN(String lsn) {
        String[] parts = lsn.split(":");
        if (parts.length != 3) {
            return null;
        }
        return new BigInteger("0").add(BigInteger.valueOf(Long.parseLong(parts[2], 16))).
                add(BigInteger.valueOf(Long.parseLong(parts[1], 16) *  100000L)).
                add(BigInteger.valueOf(Long.parseLong(parts[0], 16)).multiply(BigInteger.valueOf(1000000000000000L))).
                toByteArray();
    }
}
