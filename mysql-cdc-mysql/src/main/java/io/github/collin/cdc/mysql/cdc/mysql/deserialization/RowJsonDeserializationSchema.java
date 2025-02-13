package io.github.collin.cdc.mysql.cdc.mysql.deserialization;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.collin.cdc.common.constants.SchemaConstants;
import io.github.collin.cdc.common.enums.OpType;
import io.github.collin.cdc.common.util.JacksonUtil;
import com.ververica.cdc.connectors.mysql.debezium.dispatcher.SignalEventDispatcher;
import com.ververica.cdc.connectors.shaded.com.google.common.collect.Maps;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Schema;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Struct;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.json.DecimalFormat;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.json.JsonConverterConfig;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.source.SourceRecord;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.storage.ConverterConfig;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.storage.ConverterType;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import io.github.collin.cdc.mysql.cdc.common.dto.RowJson;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.Map;
import java.util.TimeZone;

/**
 * 自定义 JSON 序列化器，{@link RowJson}，提取变更内容为 json 数据。
 *
 * @author Li.Wei by 2022/6/14
 */
@Slf4j
public class RowJsonDeserializationSchema implements DebeziumDeserializationSchema<RowJson> {

    private transient NornsJsonConverter jsonConverter;
    /**
     * 目标库时区
     */
    private String timeZone;
    /**
     * 目标库与源库时区差（单位毫秒）
     */
    private int zoneDif;
    private final Map<String, Object> customConverterConfigs = Maps.newHashMap();
    private transient TypeInformation<RowJson> producedType = TypeInformation.of(RowJson.class);

    public RowJsonDeserializationSchema(String timeZone, int zoneDif) {
        this(JsonConverterConfig.SCHEMAS_ENABLE_DEFAULT, timeZone, zoneDif);
    }

    public RowJsonDeserializationSchema(boolean includeSchema, String timeZone, int zoneDif) {
        this(includeSchema, Collections.emptyMap());
        this.timeZone = timeZone;
        this.zoneDif = zoneDif;
    }

    public RowJsonDeserializationSchema(boolean includeSchema, Map<String, Object> customConverterConfigs) {
        this.customConverterConfigs.put(ConverterConfig.TYPE_CONFIG, ConverterType.VALUE.getName());
        this.customConverterConfigs.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, includeSchema);
        this.customConverterConfigs.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, DecimalFormat.NUMERIC.name());
        this.customConverterConfigs.putAll(customConverterConfigs);
    }

    protected NornsJsonConverter initializeJsonConverter(Map<String, Object> customConverterConfigs) {
        final NornsJsonConverter jsonConverter = new NornsJsonConverter();
        NornsJsonConverter.setTimeZoneInfo(TimeZone.getTimeZone(timeZone), zoneDif);
        jsonConverter.configure(customConverterConfigs);
        return jsonConverter;
    }

    @Override
    public void deserialize(SourceRecord sr, Collector<RowJson> out) throws Exception {
        if (jsonConverter == null) {
            jsonConverter = initializeJsonConverter(customConverterConfigs);
        }

        Envelope.Operation op = Envelope.operationFor(sr);
        Struct value = (Struct) sr.value();
        final Struct source = value.getStruct(Envelope.FieldName.SOURCE);
        String db = source.getString(SignalEventDispatcher.DATABASE_NAME);
        String table = source.getString(SignalEventDispatcher.TABLE_NAME);
        Schema valueSchema = sr.valueSchema();
        if (op == null) {
            String historyRecordJson = value.getString(SchemaConstants.HISTORY_RECORD);
            JsonNode historyRecordJsonNode = JacksonUtil.parse(historyRecordJson);
            String ddl = historyRecordJsonNode.get(SchemaConstants.DDL).asText();
            out.collect(new RowJson()
                    .setDb(db)
                    .setTable(table)
                    .setOp(OpType.DDL)
                    .setDdl(ddl));
        } else if (op != Envelope.Operation.CREATE && op != Envelope.Operation.READ) {
            if (op == Envelope.Operation.DELETE) {
                out.collect(new RowJson()
                        .setDb(db)
                        .setTable(table)
                        .setOp(OpType.DELETE)
                        .setJson(extractBeforeRow(value, valueSchema)));
            } else {
                // 有主键，可不需要UPDATE_BEFORE
                //out.collect(new RowJson(db, table, OpType.UPDATE_BEFORE, extractBeforeRow(value, valueSchema), null));
                out.collect(new RowJson()
                        .setDb(db)
                        .setTable(table)
                        .setOp(OpType.UPDATE_AFTER)
                        .setJson(extractAfterRow(value, valueSchema)));
            }
        } else {
            out.collect(new RowJson()
                    .setDb(db)
                    .setTable(table)
                    .setOp(OpType.INSERT)
                    .setJson(extractAfterRow(value, valueSchema)));
        }
    }

    private Map<String, Object> extractAfterRow(Struct value, Schema valueSchema) {
        return jsonConverter.fromConnectData(valueSchema.field(Envelope.FieldName.AFTER).schema(), value.getStruct(Envelope.FieldName.AFTER));
    }

    private Map<String, Object> extractBeforeRow(Struct value, Schema valueSchema) {
        return jsonConverter.fromConnectData(valueSchema.field(Envelope.FieldName.BEFORE).schema(), value.getStruct(Envelope.FieldName.BEFORE));
    }

    @Override
    public TypeInformation<RowJson> getProducedType() {
        return producedType;
    }

}