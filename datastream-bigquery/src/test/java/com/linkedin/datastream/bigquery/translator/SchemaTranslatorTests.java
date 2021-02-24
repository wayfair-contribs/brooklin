/*
 * Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */

package com.linkedin.datastream.bigquery.translator;

import java.io.IOException;
import java.io.InputStream;

import org.testng.annotations.Test;

import static org.testng.Assert.assertNotNull;

@Test
public class SchemaTranslatorTests {

    @Test
    public void testLoadComplexSchema() throws IOException {
        org.apache.avro.Schema avroSchema;
        try (final InputStream avroFileInputStream = getClass().getClassLoader().getResourceAsStream("complex_avro_schema.avsc")) {
            avroSchema = new org.apache.avro.Schema.Parser().parse(avroFileInputStream);
        }
        final com.google.cloud.bigquery.Schema bqSchema = SchemaTranslator.translate(avroSchema);
        assertNotNull(bqSchema);
    }

    @Test(expectedExceptions = StackOverflowError.class)
    public void testRecursiveSchemaStackOverflowException() {
        final String recursiveSchema = "{" +
                "  \"type\": \"record\"," +
                "  \"name\": \"TreeNode\"," +
                "  \"fields\": [" +
                "    {" +
                "      \"name\": \"value\"," +
                "      \"type\": \"long\"" +
                "    }," +
                "    {" +
                "      \"name\": \"children\"," +
                "      \"type\": { \"type\": \"array\", \"items\": \"TreeNode\" }" +
                "    }" +
                "  ]" +
                "}";
        final org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(recursiveSchema);
        SchemaTranslator.translate(avroSchema);
    }
}
