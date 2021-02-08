/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.bigquery.schema;

/**
 * An enumeration of BigQuerySchemaEvolver types.
 */
public enum BigquerySchemaEvolverType {
    dynamic("DynamicSchema"),
    fixed("FixedSchema");

    private final String _modeName;

    BigquerySchemaEvolverType(final String modeName) {
        this._modeName = modeName;
    }

    public String getModeName() {
        return _modeName;
    }
}
