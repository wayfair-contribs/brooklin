package com.linkedin.datastream.connectors.jdbc.cdc;

import com.linkedin.datastream.common.translator.SchemaTranslator;
import com.linkedin.datastream.common.translator.TranslatorConstants;
import com.linkedin.datastream.connectors.jdbc.JDBCColumn;
import com.linkedin.datastream.connectors.jdbc.translator.AvroFieldSchemaBuilder;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import java.sql.Types;

import static com.linkedin.datastream.connectors.jdbc.translator.TranslatorConstants.*;
import static java.sql.Types.INTEGER;
import static java.sql.Types.TIMESTAMP;

public class CDCSchemaTranslator implements SchemaTranslator<ChangeEvent, Schema> {
    String _cdcInstanceName;

    AvroFieldSchemaBuilder _fieldSchemaBuilder = new AvroFieldSchemaBuilder();

    public CDCSchemaTranslator(String _cdcInstanceName) {
        this._cdcInstanceName = _cdcInstanceName;
    }

    @Override
    public Schema translateSchemaToInternalFormat(ChangeEvent event) {
        SchemaBuilder.FieldAssembler<Schema> rootAssembler =
                SchemaBuilder.record(_cdcInstanceName).namespace(AVRO_SCHEMA_NAMESPACE).fields();

        Schema valueSchema = buildValueSchema(event);
        Schema sourceSchema = buildSourceSchema(event);
        // todo: Transaction schema

        return rootAssembler
                .name("before").type().optional().type(valueSchema)
                .name("after").type().optional().type(valueSchema)
                .name("source").type(sourceSchema).noDefault()
                .endRecord();
    }

    private Schema buildSourceSchema(ChangeEvent event) {
        SchemaBuilder.FieldAssembler<Schema> assembler = SchemaBuilder.record("Source").fields();

        JDBCColumn commitLsn = JDBCColumn.JDBCColumnBuilder.builder()
                .name(AVRO_FIELD_COMMIT_LSN)
                .sqlType(DECIMAL_BYTES_TYPE).precision(24).scale(0) // todo should be string??
                .optional(false)
                .build();
        _fieldSchemaBuilder.build(commitLsn, assembler);

        JDBCColumn changeLsn = JDBCColumn.JDBCColumnBuilder.builder()
                .name(AVRO_FIELD_CHANGE_LSN)
                .sqlType(DECIMAL_BYTES_TYPE).precision(24).scale(0)
                .optional(false)
                .build();
        _fieldSchemaBuilder.build(changeLsn, assembler);

        JDBCColumn eventSeqNo = JDBCColumn.JDBCColumnBuilder.builder()
                .name(AVRO_FIELD_EVENT_SERIAL_NO)
                .sqlType(INTEGER)
                .optional(false)
                .build();
        _fieldSchemaBuilder.build(eventSeqNo, assembler);

        JDBCColumn op = JDBCColumn.JDBCColumnBuilder.builder()
                .name(AVRO_FIELD_OP)
                .sqlType(CDC_OP_TYPE)
                .optional(false)
                .build();
        _fieldSchemaBuilder.build(op, assembler);

        JDBCColumn ts_ms = JDBCColumn.JDBCColumnBuilder.builder()
                .name(AVRO_FIELD_TS_MS)
                .sqlType(TIMESTAMP)
                .optional(false)
                .build();
        _fieldSchemaBuilder.build(ts_ms, assembler);

        return assembler.endRecord();
    }

    private Schema buildValueSchema(ChangeEvent event) {
        SchemaBuilder.FieldAssembler<Schema> assembler = SchemaBuilder.record("Value").fields();

        // DataAfter should always have data
        for (JDBCColumn column: event.getDataAfter()) {
            _fieldSchemaBuilder.build(column, assembler);
        }

        return assembler.endRecord();
    }

    public static Schema getValueSchema(Schema root) {
        // before record is union.
        return root.getField("before").schema().getTypes().get(1);
    }

    public static Schema getSourceSchema(Schema root) {
        return root.getField("source").schema();
    }
}
