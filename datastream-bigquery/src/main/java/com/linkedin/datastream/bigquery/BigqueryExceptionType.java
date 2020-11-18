package com.linkedin.datastream.bigquery;

public enum BigqueryExceptionType {
    SchemaEvolution,
    Deserialization,
    SchemaTranslation,
    InsertError,
    Other
}
