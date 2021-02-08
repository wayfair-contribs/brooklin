package com.linkedin.datastream.connectors.jdbc.cdc;

public interface CDCQueryBuilder {
    final String PLACE_HOLDER = "#";
    int DATA_COLUMN_START_INDEX = 7;
    int META_COLUMN_NUM = 7;

    int OP_INDEX = 5;
    int SEQVAL_INDEX = 4;
    int LSN_INDEX = 2;
    int TRANSACTION_TIME_INDEX = 1;

    String generate();
}
