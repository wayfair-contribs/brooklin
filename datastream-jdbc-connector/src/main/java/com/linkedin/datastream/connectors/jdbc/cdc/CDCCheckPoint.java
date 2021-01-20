package com.linkedin.datastream.connectors.jdbc.cdc;

import org.jetbrains.annotations.NotNull;

import javax.rmi.CORBA.Util;
import java.util.Arrays;

public class CDCCheckPoint implements Comparable<CDCCheckPoint> {

    private String captureInstName;
    private byte[] lsn;
    private byte[] seqVal;

    private int[] lsnUnsignedBinary;
    private int[] seqValUnsignedBinary;


    public CDCCheckPoint(String captureInstName, byte[] lsn, byte[] seqVal) {
        this.captureInstName = captureInstName;
        this.lsn = lsn;
        this.seqVal = seqVal;
    }

    @Override
    public int compareTo(@NotNull CDCCheckPoint ckpt) {
        return compare(ckpt.lsn, ckpt.seqVal);
    }

    @Override
    public String toString() {
        String lsnStr = lsn == null ? "null" : Utils.bytesToHex(lsn);
        String seqStr = seqVal == null ? "null" : Utils.bytesToHex(seqVal);
        return lsnStr + "-" + seqStr;
    }

    public int compare(byte[] otherLsn, byte[] otherSeqVal) {
        int lsnCmp = compareLsn(otherLsn);

        // LSN not equal, return
        if (lsnCmp != 0) {
            return lsnCmp;
        }
        // LSN equal, compare seqVal
        int seqCmp = compareSeqVal(otherSeqVal);
        return seqCmp;
    }

    private int compareLsn(byte[] otherLsn) {
        final int[] thisU = lsnUnsignedBinary();
        final int[] thatU = getUnsignedBinary(otherLsn);
        for (int i = 0; i < thisU.length; i++) {
            final int diff = thisU[i] - thatU[i];
            if (diff != 0) {
                return diff;
            }
        }
        return 0;
    }

    private int compareSeqVal(byte[] otherSeqVal) {
        final int[] thisU = seqValUnsignedBinary();
        final int[] thatU = getUnsignedBinary(otherSeqVal);
        for (int i = 0; i < thisU.length; i++) {
            final int diff = thisU[i] - thatU[i];
            if (diff != 0) {
                return diff;
            }
        }
        return 0;
    }

    public byte[] lsnBytes() {
        return lsn;
    }

    public byte[] toBytes() {
        // todo
        return null;
    }


    private int[] lsnUnsignedBinary() {
        if (lsnUnsignedBinary != null || lsn == null) {
            return lsnUnsignedBinary;
        }
        lsnUnsignedBinary = getUnsignedBinary(lsn);
        return lsnUnsignedBinary;
    }

    private int[] seqValUnsignedBinary() {
        if (seqValUnsignedBinary != null || seqVal == null) {
            return seqValUnsignedBinary;
        }

        seqValUnsignedBinary = getUnsignedBinary(seqVal);
        return seqValUnsignedBinary;
    }

    private static int[] getUnsignedBinary(byte[] binary) {
        int[] unsignedBinary = new int[binary.length];
        for (int i = 0; i < binary.length; i++) {
            unsignedBinary[i] = Byte.toUnsignedInt(binary[i]);
        }
        return unsignedBinary;
    }
}
