package com.linkedin.datastream.connectors.jdbc.cdc;

import com.linkedin.datastream.connectors.jdbc.JDBCColumn;
import com.linkedin.datastream.connectors.jdbc.Utils;

import java.sql.Time;
import java.sql.Timestamp;

public class ChangeEvent {

    // meta columns
    private Timestamp txStartTime;
    private int op;
    private byte[] seqVal;
    private CDCCheckPoint checkpoint;
    private int cmdId;

    private boolean isCompleted = false;

    // data columns
    private JDBCColumn[] dataBefore;
    private JDBCColumn[] dataAfter;

    public ChangeEvent(CDCCheckPoint checkpoint, byte[] sqlVal, int op, int cmdId, Timestamp txStartTime) {
        this.checkpoint = checkpoint;
        this.op = op;
        this.seqVal = sqlVal;
        this.cmdId = cmdId;
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

    public int offset() {
        return checkpoint.offset();
    }

    public Timestamp txCommitTime() {
        return txStartTime;
    }

    public JDBCColumn[] getDataBefore() {
        return dataBefore;
    }

    public JDBCColumn[] getDataAfter() {
        return dataAfter;
    }

    public void setUpdateBefore(JDBCColumn[] dataBefore) {
        this.dataBefore = dataBefore;
        isCompleted = false;
    }

    public void setUpdateAfter(JDBCColumn[] dataAfter) {
        this.dataAfter = dataAfter;
        isCompleted = true;
    }

    public void completeUpdate(JDBCColumn[] dataAfter, byte[] seqVal, CDCCheckPoint checkpoint) {
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
                ", seqVal=" + Utils.bytesToHex(seqVal) +
                ", checkpoint=" + checkpoint +
                ", cmdId=" + cmdId +
                ", isCompleted=" + isCompleted +
                ", dataBefore=" + dataBefore +
                ", dataAfter=" + dataAfter +
                '}';
    }
}