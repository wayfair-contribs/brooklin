package com.linkedin.datastream.connectors.jdbc.cdc;

import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.metrics.DynamicMetricsManager;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

public class CDCRowReader {
    ResultSet _row;

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
        } catch (SQLException e) {
            throw new DatastreamRuntimeException(e);
        }

        DynamicMetricsManager.getInstance().createOrUpdateHistogram(
                this.getClass().getSimpleName(), "getNext", "exec_time",
                System.currentTimeMillis() - startTime);
        return hasNext;
    }

    public long readTransactionTime() {
        try {
            Timestamp ts = _row.getTimestamp(CDCQueryBuilder.TRANSACTION_TIME_INDEX);
            return ts == null ? 0 : ts.getTime();
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

    public Object[] readDataColumns() {
        try {
            int numCols = _row.getMetaData().getColumnCount();
            int size = numCols - CDCQueryBuilder.META_COLUMN_NUM;
            Object[] data = new Object[size];  // exclude 5 meta columns
            //  read columns from 6 to last-1.
            for (int i = CDCQueryBuilder.DATA_COLUMN_START_INDEX; i < numCols; i++) {
                data[i - CDCQueryBuilder.DATA_COLUMN_START_INDEX] = _row.getObject(i);
            }
            return data;
        } catch (SQLException e) {
            throw new DatastreamRuntimeException("Failed to read data columns in each row", e);
        }
    }
}
