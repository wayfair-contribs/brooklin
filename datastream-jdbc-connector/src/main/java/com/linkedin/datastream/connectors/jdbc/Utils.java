package com.linkedin.datastream.connectors.jdbc;

public class Utils {

    private final static char[] HEX_CHARS = "0123456789ABCDEF".toCharArray();

    public static String bytesToHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = HEX_CHARS[v >>> 4];
            hexChars[j * 2 + 1] = HEX_CHARS[v & 0x0F];
        }
        return String.valueOf(hexChars);
    }

    public static byte[] intToBytes(int l) {
        byte[] result = new byte[4];
        for (int i = 3; i >= 0; i--) {
            result[i] = (byte)(l & 0xFF);
            l >>= 4;
        }
        return result;
    }

    public static int bytesToInt(final byte[] b) {
        return bytesToInt(b, 0);
    }

    public static int bytesToInt(final byte[] b, final int from) {
        int result = 0;
        for (int i = from; i < from+4; i++) {
            result <<= 4;
            result |= (b[i] & 0xFF);
        }
        return result;
    }
}
