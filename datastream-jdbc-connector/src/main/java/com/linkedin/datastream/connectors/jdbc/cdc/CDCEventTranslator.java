package com.linkedin.datastream.connectors.jdbc.cdc;

import com.linkedin.datastream.common.translator.RecordTranslator;
import com.linkedin.datastream.common.translator.SchemaTranslator;
import com.linkedin.datastream.connectors.jdbc.JDBCColumn;
import com.linkedin.datastream.connectors.jdbc.Utils;
import com.linkedin.datastream.connectors.jdbc.translator.ColumnConverter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;

import static com.linkedin.datastream.connectors.jdbc.translator.TranslatorConstants.*;
import static com.linkedin.datastream.connectors.jdbc.JDBCColumn.JDBCColumnBuilder;


public class CDCEventTranslator implements RecordTranslator<ChangeEvent, GenericRecord> {
    private static final Logger LOG = LoggerFactory.getLogger(CDCEventTranslator.class);

    private String _cdcInstanceName;
    private Schema _avroSchema;
    private Schema _valueSchema;
    private Schema _sourceSchema;
    private ColumnConverter _colConverter;

    public CDCEventTranslator(String cdcInstanceName) {
        _cdcInstanceName = cdcInstanceName;
        _colConverter = new ColumnConverter();
    }

    public GenericRecord translateToInternalFormat(ChangeEvent event) {
        ensureSchema(event);

        GenericRecord rootRecord = new GenericData.Record(_avroSchema);
        GenericRecord beforeRecord = buildValueRecord(_valueSchema, event.getDataBefore());
        GenericRecord afterRecord = buildValueRecord(_valueSchema, event.getDataAfter());
        GenericRecord sourceRecord = buildSourceRecord(_sourceSchema, event);
        //todo: trasnaction

        rootRecord.put("before", beforeRecord);
        rootRecord.put("after", afterRecord);
        rootRecord.put("source", sourceRecord);

        return rootRecord;
    }

    private GenericRecord buildValueRecord(Schema valueSchema, JDBCColumn[] data) {
        if (data == null) return null;

        GenericRecord record = new GenericData.Record(_valueSchema);
        for (int i = 0; i < data.length; i++) {
            if (data[i] != null) {
                record.put(i, _colConverter.convert(data[i]));
                if (LOG.isTraceEnabled()) {
                    LOG.trace("Column before conversion =  {}, after {}", data[i], _colConverter.convert(data[i]));
                }
            }
        }

        return record;
    }

    private GenericRecord buildSourceRecord(Schema sourceSchema, ChangeEvent event) {
        GenericRecord record = new GenericData.Record(sourceSchema);

        Object commitLsnVal = _colConverter.convert(JDBCColumnBuilder.builder()
                .sqlType(DECIMAL_BYTES_TYPE)
                .value(event.getCheckpoint().lsnBytes())
                .build());
        record.put(AVRO_FIELD_COMMIT_LSN, commitLsnVal);

        Object changeLsnVal = _colConverter.convert(JDBCColumnBuilder.builder()
                .sqlType(DECIMAL_BYTES_TYPE)
                .value(event.getSeqVal())
                .build());
        record.put(AVRO_FIELD_CHANGE_LSN, changeLsnVal);

        Object eventSn = _colConverter.convert(JDBCColumnBuilder.builder()
                .sqlType(Types.INTEGER)
                .value(event.offset())
                .build());
        record.put(AVRO_FIELD_EVENT_SERIAL_NO, eventSn);

        Object opVal = _colConverter.convert(JDBCColumnBuilder.builder()
                .sqlType(CDC_OP_TYPE)
                .value(event.getOp())
                .build());
        record.put(AVRO_FIELD_OP, opVal);

        Object txCommitTime = _colConverter.convert(JDBCColumnBuilder.builder()
                .sqlType(Types.TIMESTAMP)
                .value(event.txCommitTime())
                .build());
        record.put(AVRO_FIELD_TS_MS, txCommitTime);

        return record;
    }

    private void ensureSchema(ChangeEvent event) {
        if (_avroSchema != null) {
            return;
        }

        CDCSchemaTranslator schemaTranslator = new CDCSchemaTranslator(_cdcInstanceName);
        _avroSchema = schemaTranslator.translateSchemaToInternalFormat(event);
        _valueSchema = CDCSchemaTranslator.getValueSchema(_avroSchema);
        _sourceSchema = CDCSchemaTranslator.getSourceSchema(_avroSchema);
    }
}
