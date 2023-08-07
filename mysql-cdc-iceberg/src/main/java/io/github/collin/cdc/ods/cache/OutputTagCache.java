package io.github.collin.cdc.ods.cache;

import io.github.collin.cdc.common.constants.CdcConstants;
import io.github.collin.cdc.ods.dto.RowJson;
import org.apache.flink.util.OutputTag;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class OutputTagCache {

    private static final ConcurrentMap<String, OutputTag<RowJson>> OUTPUT_TAG_CACHE = new ConcurrentHashMap<>();

    /**
     * 获取侧输出
     *
     * @param dbName 目标数据库名
     * @param table  目标表名
     * @return
     */
    public static OutputTag<RowJson> getOutputTag(String dbName, String table) {
        String outPutTagId = dbName + CdcConstants.DOT + table;
        OutputTag<RowJson> outputTag = OUTPUT_TAG_CACHE.get(outPutTagId);
        if (outputTag == null) {
            synchronized (OUTPUT_TAG_CACHE) {
                outputTag = OUTPUT_TAG_CACHE.get(outPutTagId);
                if (outputTag == null) {
                    outputTag = new OutputTag<RowJson>(outPutTagId) {
                    };
                    OUTPUT_TAG_CACHE.put(outPutTagId, outputTag);
                }
            }
        }

        return outputTag;
    }

}