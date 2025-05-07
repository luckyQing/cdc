package io.github.collin.cdc.mysql.cdc.ods.cache;

import io.github.collin.cdc.common.constants.CdcConstants;
import io.github.collin.cdc.mysql.cdc.common.dto.RowJson;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.util.OutputTag;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class OutputTagCache {

    /**
     * 库表盘路输出
     */
    private static final ConcurrentMap<String, OutputTag<RowJson>> COMMON_OUTPUT_TAG_CACHE = new ConcurrentHashMap<>();
    /**
     * mq旁路输出
     */
    private static final ConcurrentMap<String, OutputTag<String>> MQ_OUTPUT_TAG_CACHE = new ConcurrentHashMap<>();

    /**
     * 获取库表旁路输出
     *
     * @param dbName 目标数据库名
     * @param table  目标表名
     * @return
     */
    public static OutputTag<RowJson> getOutputTag(String dbName, String table) {
        String outPutTagId = dbName + CdcConstants.DOT + table;
        return getOutputTag(COMMON_OUTPUT_TAG_CACHE, RowJson.class, outPutTagId);
    }

    /**
     * 获取MQ侧输出
     *
     * @param dbName 目标数据库名
     * @param table  目标表名
     * @return
     */
    public static OutputTag<String> getMQOutputTag(String dbName, String table) {
        String outPutTagId = "mq" + CdcConstants.DOT + dbName + CdcConstants.DOT + table;
        return getOutputTag(MQ_OUTPUT_TAG_CACHE, String.class, outPutTagId);
    }

    /**
     * 获取旁路输出
     *
     * @param outputTagCache
     * @param type
     * @param outPutTagId
     * @param <T>
     * @return
     */
    public static <T> OutputTag<T> getOutputTag(ConcurrentMap<String, OutputTag<T>> outputTagCache, Class<T> type, String outPutTagId) {
        OutputTag<T> outputTag = outputTagCache.get(outPutTagId);
        if (outputTag == null) {
            synchronized (outputTagCache) {
                outputTag = outputTagCache.get(outPutTagId);
                if (outputTag == null) {
                    outputTag = new OutputTag(outPutTagId, TypeExtractor.createTypeInfo(type)) {
                    };
                    outputTagCache.put(outPutTagId, outputTag);
                }
            }
        }

        return outputTag;
    }

}