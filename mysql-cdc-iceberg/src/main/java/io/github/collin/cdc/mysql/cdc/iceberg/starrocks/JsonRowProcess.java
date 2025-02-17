package io.github.collin.cdc.mysql.cdc.iceberg.starrocks;

import io.github.collin.cdc.common.enums.OpType;
import io.github.collin.cdc.mysql.cdc.common.constants.FieldConstants;
import io.github.collin.cdc.mysql.cdc.common.dto.RowJson;
import lombok.AllArgsConstructor;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

import static com.starrocks.connector.flink.cdc.json.DebeziumJsonSerializer.INVALID_RESULT;

/**
 * 处理{@link RowJson}对象，转换为json字符串
 *
 * @author collin
 * @date 2024-12-02
 */
@AllArgsConstructor
public class JsonRowProcess extends ProcessFunction<RowJson, String> {

    private JsonRowSerializer serializer;
    /**
     * 是否分表
     */
    private boolean isSharding;

    @Override
    public void processElement(RowJson value, ProcessFunction<RowJson, String>.Context ctx, Collector<String> out) throws Exception {
        if (value.getOp() == OpType.DDL) {
            // 只同步sql变更
            serializer.process(value);
            return;
        }

        // 分库分表的数据列，添加库名、表名
        if (isSharding) {
            Map<String, Object> map = value.getJson();
            map.put(FieldConstants.DB_NAME, value.getDb());
            map.put(FieldConstants.TABLE_NAME, value.getTable());
        }

        String json = serializer.process(value);
        if (!INVALID_RESULT.equals(json)) {
            out.collect(json);
        }
    }

}