package io.github.collin.cdc.ods.function;

import io.github.collin.cdc.common.dto.MessageBodyDTO;
import io.github.collin.cdc.common.util.JacksonUtil;
import io.github.collin.cdc.ods.cache.OutputTagCache;
import io.github.collin.cdc.ods.dto.RowJson;
import lombok.RequiredArgsConstructor;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 增量数据旁路输出，以便sink到mq
 *
 * @author collin
 * @date 2023-08-16
 */
@RequiredArgsConstructor
public class SplitMQProcessFunction extends ProcessFunction<RowJson, RowJson> {

    /**
     * 目标库名
     */
    private final String dbName;
    /**
     * 目标表名
     */
    private final String tableName;

    @Override
    public void processElement(RowJson rowJson, ProcessFunction<RowJson, RowJson>.Context context, Collector<RowJson> collector) throws Exception {
        // 增量时，同时发rocketmq，此处旁路输出一个流
        if (rowJson.isIncremental()) {
            MessageBodyDTO bodyDTO = new MessageBodyDTO();
            bodyDTO.setOp(rowJson.getOp());
            bodyDTO.setRow(rowJson.getJson());

            context.output(OutputTagCache.getMQOutputTag(dbName, tableName), JacksonUtil.toJson(bodyDTO));
        }

        collector.collect(rowJson);
    }

}