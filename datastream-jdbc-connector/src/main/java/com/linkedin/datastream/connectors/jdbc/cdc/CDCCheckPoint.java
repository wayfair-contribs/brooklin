package com.linkedin.datastream.connectors.jdbc.cdc;

import com.linkedin.datastream.connectors.jdbc.Utils;
import com.linkedin.datastream.server.providers.PersistableCheckpoint;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

public class CDCCheckPoint implements PersistableCheckpoint, Comparable<CDCCheckPoint> {

    private byte[] lsn;
    private int offset;

    private int[] lsnUnsignedBinary;

    public CDCCheckPoint(byte[] lsn, int offset) {
        this.lsn = lsn;
        this.offset = offset;
    }

    public int offset() {
        return offset;
    }

    /**
     * Serialize CDC checkpoint to byte array
     * format: [lsn (10 bytes) + offset (4 bytes)]
     * @return
     */
    @Override
    public byte[] serialize() {
        // Assume lsn is never null
        byte[] buf = Arrays.copyOf(lsn, 14);
        System.arraycopy(Utils.intToBytes(offset), 0, buf, 10, 4);
        return buf;
    }

    @Override
    public int compareTo(@NotNull CDCCheckPoint checkpoint) {
        return compare(checkpoint.lsn, checkpoint.offset);
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

    public static class Deserializer implements PersistableCheckpoint.Deserializer {

        @Override
        public <T extends PersistableCheckpoint> T deserialize(byte[] value, Class<T> checkpointClass) {
            // Assume the length is 14
            byte[] lsn = Arrays.copyOfRange(value, 0, 10);
            int offset = Utils.bytesToInt(value, 10);

            return checkpointClass.cast(new CDCCheckPoint(lsn, offset));
        }
    }
}
