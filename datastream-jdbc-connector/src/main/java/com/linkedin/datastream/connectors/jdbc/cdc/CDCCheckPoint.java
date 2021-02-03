package com.linkedin.datastream.connectors.jdbc.cdc;

import org.jetbrains.annotations.NotNull;

public class CDCCheckPoint implements Comparable<CDCCheckPoint> {

    private String captureInstName;
    private byte[] lsn;
    private int offset;

    private int[] lsnUnsignedBinary;

    public CDCCheckPoint(String captureInstName, byte[] lsn, int offset) {
        this.captureInstName = captureInstName;
        this.lsn = lsn;
        this.offset = offset;
    }

    public void incrementOffset() {
        offset++;
    }

    public void resetOffset() {
        offset = 0;
    }

    public int offset() {
        return offset;
    }

    @Override
    public int compareTo(@NotNull CDCCheckPoint ckpt) {
        return compare(ckpt.lsn, ckpt.offset);
    }

    @Override
    public String toString() {
        String lsnStr = (lsn == null) ? "null" : Utils.bytesToHex(lsn);
        return lsnStr + ":" + offset;
    }

    public int compare(byte[] lsn2, int offset2) {
        int lsnCmp = compareLsn(lsn2);

        // LSN not equal, return
        if (lsnCmp != 0) {
            return lsnCmp;
        }

        return offset - offset2;
    }

    private int compareLsn(byte[] lsn2) {
        final int[] thisU = lsnUnsignedBinary();
        final int[] thatU = getUnsignedBinary(lsn2);
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

    private int[] lsnUnsignedBinary() {
        if (lsnUnsignedBinary != null || lsn == null) {
            return lsnUnsignedBinary;
        }
        lsnUnsignedBinary = getUnsignedBinary(lsn);
        return lsnUnsignedBinary;
    }

    private static int[] getUnsignedBinary(byte[] binary) {
        int[] unsignedBinary = new int[binary.length];
        for (int i = 0; i < binary.length; i++) {
            unsignedBinary[i] = Byte.toUnsignedInt(binary[i]);
        }
        return unsignedBinary;
    }
}
