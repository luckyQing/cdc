package io.github.collin.cdc.mysql.cdc.iceberg.function;

import io.github.collin.cdc.common.enums.OpType;
import io.github.collin.cdc.common.util.JacksonUtil;
import io.github.collin.cdc.mysql.cdc.common.dto.RowJson;
import io.github.collin.cdc.mysql.cdc.iceberg.constants.FieldConstants;
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
            Map<String, Object> map = value.getJson();
            map.put(FieldConstants.DB_NAME, value.getDb());
            map.put(FieldConstants.TABLE_NAME, value.getTable());
            json = JacksonUtil.toBytes(map);
        } else {
            json = JacksonUtil.toBytes(value.getJson());
        }

        RowData rowData = deserializationSchema.deserialize(json);
        rowData.setRowKind(OpType.convertRowKind(value.getOp()));

        out.collect(rowData);

        // 及时释放
        json = null;

        value.setJson(null);
        value.setDb(null);
        value.setTable(null);
    }

}