/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common.translator;

/**
 * Interface to translate schema from one format into another format
 * @param <ST>
 * @param <DT>
 */
public interface SchemaTranslator<ST, DT> {
    /**
     * Translates values from internal format into T format
     *
     * @param sourceRecord - source schema
     * @return The translated record in T format
     */
    default DT translateSchema(ST sourceRecord) throws Exception {
        return null;
    }
}
