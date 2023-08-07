package io.github.collin.cdc.ods.function;

import com.fasterxml.jackson.core.type.TypeReference;
import io.github.collin.cdc.common.util.JacksonUtil;
import io.github.collin.cdc.ods.constants.FieldConstants;
import io.github.collin.cdc.ods.dto.RowJson;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;

import java.util.Map;

/**
 * {@link RowJson}转{@link RowData}
 *
 * @author collin
 * @date 2023-04-26
 */
public class RowJsonConvertFunction extends RichFlatMapFunction<RowJson, RowData> {

    private final RowType rowType;
    /**
     * 是否分表
     */
    private boolean isSharding;
    private transient JsonRowDataDeserializationSchema deserializationSchema;

    public RowJsonConvertFunction(RowType rowType, boolean isSharding) {
        this.rowType = rowType;
        this.isSharding = isSharding;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        this.deserializationSchema = new JsonRowDataDeserializationSchema(rowType, InternalTypeInfo.of(rowType),
                false, false, TimestampFormat.ISO_8601);
        this.deserializationSchema.open(null);
    }

    @Override
    public void flatMap(RowJson value, Collector<RowData> out) throws Exception {
        byte[] json = null;
        // 分库分表的数据列，添加库名、表名
        if (isSharding) {
            Map<String, Object> map = JacksonUtil.parseJsonBytes(value.getJson(), new TypeReference<Map<String, Object>>() {
            });
            map.put(FieldConstants.DB_NAME, value.getDb());
            map.put(FieldConstants.TABLE_NAME, value.getTable());
            json = JacksonUtil.toBytes(map);

            map.clear();
            map = null;
        } else {
            json = value.getJson();
        }

        RowData rowData = deserializationSchema.deserialize(json);
        rowData.setRowKind(value.getOp().convertRowKind());

        out.collect(rowData);

        // 及时释放
        json = null;

        value.setJson(null);
        value.setDb(null);
        value.setTable(null);
        value.setOp(null);
    }

}