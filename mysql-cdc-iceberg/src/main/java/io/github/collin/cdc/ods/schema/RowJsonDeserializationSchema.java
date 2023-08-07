package io.github.collin.cdc.ods.schema;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Maps;
import com.ververica.cdc.connectors.mysql.debezium.dispatcher.SignalEventDispatcher;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Schema;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Struct;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.json.DecimalFormat;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.json.JsonConverter;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.json.JsonConverterConfig;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.source.SourceRecord;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.storage.ConverterConfig;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.storage.ConverterType;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import io.github.collin.cdc.common.constants.SchemaConstants;
import io.github.collin.cdc.common.util.JacksonUtil;
import io.github.collin.cdc.ods.dto.RowJson;
import io.github.collin.cdc.ods.enums.OpType;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.Map;

/**
 * 自定义 JSON 序列化器，{@link RowJson}，提取变更内容为 json 数据。
 *
 * @author Li.Wei by 2022/6/14
 */
@Slf4j
public class RowJsonDeserializationSchema implements DebeziumDeserializationSchema<RowJson> {

    private transient JsonConverter jsonConverter;
    private final Map<String, Object> customConverterConfigs = Maps.newHashMap();
    private transient TypeInformation<RowJson> producedType = TypeInformation.of(RowJson.class);

    public RowJsonDeserializationSchema() {
        this(JsonConverterConfig.SCHEMAS_ENABLE_DEFAULT);
    }

    public RowJsonDeserializationSchema(boolean includeSchema) {
        this(includeSchema, Collections.emptyMap());
    }

    public RowJsonDeserializationSchema(boolean includeSchema, Map<String, Object> customConverterConfigs) {
        this.customConverterConfigs.put(ConverterConfig.TYPE_CONFIG, ConverterType.VALUE.getName());
        this.customConverterConfigs.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, includeSchema);
        this.customConverterConfigs.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, DecimalFormat.NUMERIC.name());
        this.customConverterConfigs.putAll(customConverterConfigs);
    }

    protected JsonConverter initializeJsonConverter(Map<String, Object> customConverterConfigs) {
        final JsonConverter jsonConverter = new NornsJsonConverter();
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
            out.collect(new RowJson(db, table, OpType.DDL, null, ddl, JacksonUtil.toJson(sr.sourceOffset())));
        } else if (op != Envelope.Operation.CREATE && op != Envelope.Operation.READ) {
            if (op == Envelope.Operation.DELETE) {
                out.collect(new RowJson(db, table, OpType.DELETE, extractBeforeRow(sr.topic(), value, valueSchema), null, null));
            } else {
                // 有主键，可不需要UPDATE_BEFORE
                //out.collect(new RowJson(db, table, OpType.UPDATE_BEFORE, extractBeforeRow(sr.topic(), value, valueSchema), null, null));
                out.collect(new RowJson(db, table, OpType.UPDATE_AFTER, extractAfterRow(sr.topic(), value, valueSchema), null, null));
            }
        } else {
            out.collect(new RowJson(db, table, OpType.INSERT, extractAfterRow(sr.topic(), value, valueSchema), null, null));
        }
    }

    private byte[] extractAfterRow(String topic, Struct value, Schema valueSchema) {
        try {
            byte[] bytes = jsonConverter.fromConnectData(topic, valueSchema.field(Envelope.FieldName.AFTER).schema(), value.getStruct(Envelope.FieldName.AFTER));
            return bytes == null ? new byte[]{} : bytes;
        } catch (Exception e) {
            log.error("extractAfterRow value:{}, valueSchema:{}", value, valueSchema, e);
            return new byte[]{};
        }
    }

    private byte[] extractBeforeRow(String topic, Struct value, Schema valueSchema) {
        try {
            byte[] bytes = jsonConverter.fromConnectData(topic, valueSchema.field(Envelope.FieldName.BEFORE).schema(), value.getStruct(Envelope.FieldName.BEFORE));
            return bytes == null ? new byte[]{} : bytes;
        } catch (Exception e) {
            log.error("extractBeforeRow value:{}, valueSchema:{}", value, valueSchema, e);
            return new byte[]{};
        }
    }

    @Override
    public TypeInformation<RowJson> getProducedType() {
        return producedType;
    }

}