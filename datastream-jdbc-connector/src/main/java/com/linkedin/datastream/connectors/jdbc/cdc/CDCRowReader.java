package com.linkedin.datastream.connectors.jdbc.cdc;

import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.connectors.jdbc.JDBCColumn;
import com.linkedin.datastream.metrics.DynamicMetricsManager;

import java.sql.*;

public class CDCRowReader {
    ResultSet _row;
    ResultSetMetaData _schema;

    public CDCRowReader() {
    }

    /**
     * Fetch the next row from DB. Must be called before calling any read methods.
     * @param resultSet
     * @return
     */
    public boolean next(ResultSet resultSet) {
        boolean hasNext;
        long startTime = System.currentTimeMillis();
        try {
            hasNext = resultSet.next();
            _row = resultSet;
            _schema = resultSet.getMetaData();
        } catch (SQLException e) {
            throw new DatastreamRuntimeException(e);
        }

        DynamicMetricsManager.getInstance().createOrUpdateHistogram(
                this.getClass().getSimpleName(), "getNext", "exec_time",
                System.currentTimeMillis() - startTime);
        return hasNext;
    }

    public Timestamp readTransactionTime() {
        try {
            Timestamp ts = _row.getTimestamp(CDCQueryBuilder.TRANSACTION_TIME_INDEX);
            return ts;
        } catch (SQLException e) {
            throw new DatastreamRuntimeException(e);
        }
    }

    public int readOperation() {
        try {
            return _row.getInt(CDCQueryBuilder.OP_INDEX);
        } catch (SQLException e) {
            throw new DatastreamRuntimeException(e);
        }
    }

    public byte[] readSeqVal() {
        try {
            return _row.getBytes(CDCQueryBuilder.SEQVAL_INDEX);
        } catch (SQLException e) {
            throw new DatastreamRuntimeException(e);
        }
    }

    public JDBCColumn[] readDataColumns() {
        try {
            int numCols = _row.getMetaData().getColumnCount();
            int size = numCols - CDCQueryBuilder.META_COLUMN_NUM;
            JDBCColumn[] columns = new JDBCColumn[size];  // exclude 5 meta columns
            //  read columns from 6 to last-1.
            for (int i = CDCQueryBuilder.DATA_COLUMN_START_INDEX; i < numCols; i++) {
                columns[i - CDCQueryBuilder.DATA_COLUMN_START_INDEX] =
                        JDBCColumn.JDBCColumnBuilder.builder()
                                .name(_schema.getColumnName(i))
                                .sqlType(_schema.getColumnType(i))
                                .value(_row.getObject(i))
                                .precision(_schema.getPrecision(i))
                                .scale(_schema.getScale(i))
                                .optional(_schema.isNullable(i) == 1)
                                .build();
            }
            return columns;
        } catch (SQLException e) {
            throw new DatastreamRuntimeException("Failed to read data columns in each row", e);
        }
    }

    public int readCommandId() {
        try {
            return _row.getInt("__$command_id");
        } catch (SQLException e) {
            throw new DatastreamRuntimeException(e);
        }
    }
}
