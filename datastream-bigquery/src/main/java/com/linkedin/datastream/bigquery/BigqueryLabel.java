/*
 * Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */

package com.linkedin.datastream.bigquery;

import java.util.Objects;

import org.apache.commons.lang3.Validate;

/**
 * A class representing a label on a BigQuery resource.
 */
public class BigqueryLabel {
    private final String _name;
    private final String _value;

    /**
     * Constructor.
     * @param name a String
     * @param value a String
     */
    public BigqueryLabel(final String name, final String value) {
        Validate.notBlank(name, "name cannot be blank");
        Validate.notNull(value, "value cannot be null");
        if (!value.isEmpty()) {
            Validate.notBlank(value, "value must not be blank when not empty");
        }
        Validate.isTrue(name.equals(name.toLowerCase()), "name must be lowercase");
        Validate.isTrue(value.equals(value.toLowerCase()), "value must be lowercase");
        _name = name;
        _value = value;
    }

    /**
     * Constructor.
     * @param name a String
     */
    public BigqueryLabel(final String name) {
        this(name, "");
    }

    public String getName() {
        return _name;
    }

    public String getValue() {
        return _value;
    }

    /**
     * Construct a BigqueryLabel with the given name.
     * @param name a String
     * @return a BigqueryLabel
     */
    public static BigqueryLabel of(final String name) {
        return new BigqueryLabel(name);
    }

    /**
     * Construct a BigqueryLabel with the given name and value.
     * @param name a String
     * @param value a String
     * @return the BigqueryLabel
     */
    public static BigqueryLabel of(final String name, final String value) {
        return new BigqueryLabel(name, value);
    }

    @Override
    public String toString() {
        final String label;
        if (!_value.isEmpty()) {
            label = String.join(":", _name, _value);
        } else {
            label = _name;
        }
        return label;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        } else if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final BigqueryLabel label = (BigqueryLabel) o;
        return _name.equals(label._name) && _value.equals(label._value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(_name, _value);
    }
}
