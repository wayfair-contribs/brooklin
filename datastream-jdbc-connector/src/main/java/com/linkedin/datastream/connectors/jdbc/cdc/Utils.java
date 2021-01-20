package com.linkedin.datastream.connectors.jdbc.cdc;

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

    public static String toHexString(byte[] byteArray) {
        StringBuffer hexStringBuffer = new StringBuffer();
        for (int i = 0; i < byteArray.length; i++) {
            hexStringBuffer.append(String.format("%2x", byteArray[i]));
        }
        return hexStringBuffer.toString();
    }
}
