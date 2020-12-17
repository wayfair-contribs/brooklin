/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.bigquery.schema;

import com.google.cloud.bigquery.Schema;

/**
 * A NoOp implementation of a BigquerySchemaEvolver.
 */
public class NoOpBigquerySchemaEvolver implements BigquerySchemaEvolver {

    protected NoOpBigquerySchemaEvolver() { }

    @Override
    public Schema evolveSchema(final Schema baseSchema, final Schema newSchema) throws IncompatibleSchemaEvolutionException {
        return baseSchema;
    }

}
