package com.linkedin.datastream.connectors.jdbc.cdc;

import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.server.providers.PersistableCheckpoint;
import org.jetbrains.annotations.NotNull;

import java.nio.charset.Charset;
import java.nio.charset.UnsupportedCharsetException;

public class JDBCCheckpoint implements PersistableCheckpoint, Comparable<JDBCCheckpoint> {
    long _offset = 1;

    public JDBCCheckpoint(long offset) {
        _offset = offset;
    }

    @Override
    public byte[] serialize() {
        return String.valueOf(_offset).getBytes();
    }

    @Override
    public int compareTo(@NotNull JDBCCheckpoint o) {
        return Long.compare(this._offset, o._offset);
    }

    public Long offset() {
        return _offset;
    }

    public static class Deserializer implements PersistableCheckpoint.Deserializer {

        @Override
        public <T extends PersistableCheckpoint> T deserialize(byte[] value, Class<T> checkpointClass) {
            long offset = -1;
            try {
                // Old version of checkpoint uses the string representation of Long. To be compatible still use same format.
                offset = Long.valueOf(new String(value, Charset.defaultCharset()));
            } catch (NumberFormatException | UnsupportedCharsetException e) {
                throw new DatastreamRuntimeException("Invalid CDC checkpoint offset " + value);
            }

            return checkpointClass.cast(new JDBCCheckpoint(offset));
        }
    }
}
