package com.linkedin.datastream.connectors.jdbc.cdc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

public class ChangeEvent {
    byte[] lsn;
    byte[] seq;
    int op;

    Object[] dataBefore;
    Object[] dataAfter;


    public ChangeEvent(byte[] lsn, byte[] seq, int op) {
        this.lsn = lsn;
        this.seq = seq;
        this.op = op;
    }

    @Override
    public String toString() {
        return "ChangeEvent{" +
                "lsn=" + Utils.bytesToHex(lsn) +
                ", seq=" + Utils.bytesToHex(seq) +
                ", op=" + op +
                '}';
    }
}