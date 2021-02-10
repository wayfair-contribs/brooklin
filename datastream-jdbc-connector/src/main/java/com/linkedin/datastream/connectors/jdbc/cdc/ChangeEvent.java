package com.linkedin.datastream.connectors.jdbc.cdc;

import com.linkedin.datastream.common.DatastreamRuntimeException;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

public class ChangeEvent {

    // meta columns
    private long txStartTime;
    private int op;
    private byte[] seqVal;
    private CDCCheckPoint checkpoint;

    private boolean isCompleted = false;

    // data columns
    private Object[] dataBefore;
    private Object[] dataAfter;

    public ChangeEvent(CDCCheckPoint checkpoint, byte[] sqlVal, int op, long txStartTime) {
        this.checkpoint = checkpoint;
        this.op = op;
        this.seqVal = sqlVal;
        this.txStartTime = txStartTime;
    }

    public CDCCheckPoint getCheckpoint() {
        return checkpoint;
    }

    public int getOp() {
        return op;
    }

    public byte[] getSeqVal() {
        return seqVal;
    }

    public void setSeqVal(byte[] seqVal) {
        this.seqVal = seqVal;
    }

    public long getTxStartTime() {
        return txStartTime;
    }

    public Object[] getDataBefore() {
        return dataBefore;
    }

    public Object[] getDataAfter() {
        return dataAfter;
    }

    public void setUpdateBefore(Object[] dataBefore) {
        this.dataBefore = dataBefore;
        isCompleted = false;
    }

    public void completeUpdate(Object[] dataAfter, byte[] seqVal, CDCCheckPoint checkpoint) {
        this.checkpoint = checkpoint;
        this.dataAfter = dataAfter;
        this.seqVal = seqVal;
        this.isCompleted = true;
        this.op = 4;
    }

    public boolean isComplete() {
        return isCompleted;
    }

    @Override
    public String toString() {
        return "ChangeEvent{" +
                "txStartTime=" + txStartTime +
                ", op=" + op +
                ", seqVal=" + Arrays.toString(seqVal) +
                ", checkpoint=" + checkpoint +
                ", isCompleted=" + isCompleted +
                ", dataBefore=" + dataBefore +
                ", dataAfter=" + dataAfter +
                '}';
    }
}