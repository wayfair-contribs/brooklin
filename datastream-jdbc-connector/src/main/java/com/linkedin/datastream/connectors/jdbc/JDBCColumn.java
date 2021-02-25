package com.linkedin.datastream.connectors.jdbc;

public class JDBCColumn {

    private String name;
    private int schemaSize;
    private int size;
    private int sqlType;

    @Override
    public String toString() {
        return "JDBCColumn{" +
                "name='" + name + '\'' +
                ", schemaSize=" + schemaSize +
                ", size=" + size +
                ", sqlType=" + sqlType +
                ", value=" + value +
                ", precision=" + precision +
                ", scale=" + scale +
                ", optional=" + optional +
                '}';
    }

    private Object value;
    private int precision;
    private int scale;

    private boolean optional = true;

    private JDBCColumn(String name, int schemaSize, int sqlType, Object value, int precision, int scale, boolean optional) {
        this.name = name;
        this.schemaSize = schemaSize;
        this.sqlType = sqlType;
        this.value = value;
        this.precision = precision;
        this.scale = scale;
        this.optional = optional;
    }

    public String name() {
        return name;
    }

    public int schemaSize() {
        return schemaSize;
    }

    public int sqlType() {
        return sqlType;
    }

    public Object value() {
        return value;
    }

    public int precision() {
        return precision;
    }

    public int scale() {
        return scale;
    }

    public boolean optional() {
        return optional;
    }

    public static class JDBCColumnBuilder {

        private String name;
        private int schemaSize;
        private int sqlType;
        private Object value;
        private int precision;
        private int scale;
        private boolean optional = true;

        private JDBCColumnBuilder() {

        }

        public static JDBCColumnBuilder builder() {
            return new JDBCColumnBuilder();
        }

        public JDBCColumnBuilder name(String name) {
            this.name = name;
            return this;
        }

        public JDBCColumnBuilder schemaSize(int schemaSize) {
            this.schemaSize = schemaSize;
            return this;
        }

        public JDBCColumnBuilder sqlType(int sqlType) {
            this.sqlType = sqlType;
            return this;
        }

        public JDBCColumnBuilder value(Object value) {
            this.value = value;
            return this;
        }

        public JDBCColumnBuilder precision(int precision) {
            this.precision = precision;
            return this;
        }

        public JDBCColumnBuilder scale(int scale) {
            this.scale = scale;
            return this;
        }

        public JDBCColumnBuilder optional(boolean opt) {
            this.optional = opt;
            return this;
        }

        public JDBCColumn build() {
            return new JDBCColumn(name, schemaSize, sqlType, value, precision, scale, optional);
        }
    }
}
