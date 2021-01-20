package com.linkedin.datastream.connectors.jdbc.cdc;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class AvroRecordTranslator {
    private final static Logger LOG = LoggerFactory.getLogger(AvroRecordTranslator.class);

    public static GenericRecord fromChangeEvent(Schema avroSchema, ChangeEvent event) {
        GenericRecord record = new GenericData.Record(avroSchema);

        // dataBefore could be null, dataAfter always has data
        int dataFieldsLen = event.dataAfter.length;

        int fieldIdx = 0;

        // data before
        int endIdx = dataFieldsLen;
        if (event.dataBefore != null) {
            for (; fieldIdx < endIdx; fieldIdx++) {
                record.put(fieldIdx, event.dataBefore[fieldIdx]);
            }
        } else {
            fieldIdx += dataFieldsLen;
        }

        // dataAfter
        endIdx += dataFieldsLen;
        for (; fieldIdx < endIdx; fieldIdx++) {
            record.put(fieldIdx, event.dataAfter[fieldIdx-dataFieldsLen]);
        }

        // meta columns
        record.put(fieldIdx++, ByteBuffer.wrap(event.lsn) );
        record.put(fieldIdx++, ByteBuffer.wrap(event.seq) );
        record.put(fieldIdx++, event.op);

        return record;
    }
}
