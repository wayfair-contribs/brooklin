package com.linkedin.datastream.connectors.jdbc.translator;

import com.linkedin.datastream.connectors.jdbc.cdc.ChangeEvent;
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
        int dataFieldsLen = event.getDataAfter().length;

        int fieldIdx = 0;

        // data before
        int endIdx = dataFieldsLen;
        if (event.getDataBefore() != null) {
            for (; fieldIdx < endIdx; fieldIdx++) {
                record.put(fieldIdx, event.getDataBefore()[fieldIdx]);
            }
        } else {
            fieldIdx += dataFieldsLen;
        }

        // dataAfter
        endIdx += dataFieldsLen;
        for (; fieldIdx < endIdx; fieldIdx++) {
            record.put(fieldIdx, event.getDataAfter()[fieldIdx-dataFieldsLen]);
        }

        // meta columns
        record.put(fieldIdx++, ByteBuffer.wrap(event.getCheckpoint().lsnBytes()) );
        record.put(fieldIdx++, ByteBuffer.wrap(event.getSeqVal()) );
        record.put(fieldIdx++, event.getOp());

        return record;
    }
}
