package com.linkedin.datastream.connectors.jdbc.cdc;

public class CDCQueryBuilderWithOffset implements CDCQueryBuilder {
    private static final String QUERY =
            "SELECT * FROM cdc.#_CT WITH (NOLOCK)" +
                    "WHERE __$start_lsn >= ? " +
                    "ORDER BY __$start_lsn " +
                    "OFFSET ? ROWS " +
                    "FETCH NEXT ? ROWS ONLY ";

    private static final String QUERY_WITH_TS =
            "SELECT sys.fn_cdc_map_lsn_to_time( __$start_lsn) as lsn_ts, *" +
                    "FROM cdc.#_CT WITH (NOLOCK)" +
                    "WHERE __$start_lsn >= ? " +
                    "ORDER BY __$start_lsn " +
                    "OFFSET ? ROWS " +
                    "FETCH NEXT ? ROWS ONLY ";

    private String captureInstanceName;
    private boolean withTimestamp;

    public CDCQueryBuilderWithOffset(String captureInstanceName, boolean withTimestamp) {
        this.captureInstanceName = captureInstanceName;
        this.withTimestamp = withTimestamp;
    }

    @Override
    public String generate() {
        String q = withTimestamp ? QUERY : QUERY_WITH_TS;
        return q.replace(PLACE_HOLDER, captureInstanceName);
    }
}
