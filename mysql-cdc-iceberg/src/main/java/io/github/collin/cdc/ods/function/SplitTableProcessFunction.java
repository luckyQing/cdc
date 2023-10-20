package io.github.collin.cdc.ods.function;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.io.FileUtil;
import cn.hutool.core.util.RandomUtil;
import com.fasterxml.jackson.core.type.TypeReference;
import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import io.github.collin.cdc.common.common.adapter.RedisAdapter;
import io.github.collin.cdc.common.dto.cache.ApplicationDTO;
import io.github.collin.cdc.common.enums.OpType;
import io.github.collin.cdc.common.util.JacksonUtil;
import io.github.collin.cdc.common.util.RedisKeyUtil;
import io.github.collin.cdc.ods.adapter.RobotAdapter;
import io.github.collin.cdc.ods.cache.OutputTagCache;
import io.github.collin.cdc.ods.dto.RowJson;
import io.github.collin.cdc.ods.dto.cache.DdlDTO;
import io.github.collin.cdc.ods.dto.cache.PropertiesCacheDTO;
import io.github.collin.cdc.ods.util.DdlUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.redisson.api.RMap;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RSet;
import org.redisson.api.RedissonClient;

import java.io.File;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 库表旁路输出
 *
 * @author collin
 * @date 2023-05-16
 */
@Slf4j
public class SplitTableProcessFunction extends ProcessFunction<RowJson, RowJson> {

    /**
     * 数据库及表映射关系缓存文件名
     */
    private final String relationCacheFileName;
    /**
     * properties缓存文件名
     */
    private final String propertiesCacheFileName;
    /**
     * 自定义任务名
     */
    private final String application;
    /**
     * 映射关系
     * <p>
     * 数据库：<源数据库名, 目标数据库名><br>
     * 表：<源数据库名.原表名, 目标表名>
     */
    private transient ConcurrentMap<String, String> relationLocalCache;

    private transient RobotAdapter robotAdapter;
    private transient RedisAdapter redisAdapter;

    public SplitTableProcessFunction(String application, String relationCacheFileName, String propertiesCacheFileName) {
        this.application = application;
        this.relationCacheFileName = relationCacheFileName;
        this.propertiesCacheFileName = propertiesCacheFileName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // 本地缓存映射关系
        initRelations();

        // 读取properties
        File propertiesFile = getRuntimeContext().getDistributedCache().getFile(propertiesCacheFileName);
        String propertiesJson = FileUtil.readUtf8String(propertiesFile);
        PropertiesCacheDTO propertiesCache = JacksonUtil.parseObject(propertiesJson, PropertiesCacheDTO.class);
        this.robotAdapter = new RobotAdapter(propertiesCache.getProxy(), propertiesCache.getMonitor());
        this.redisAdapter = new RedisAdapter(propertiesCache.getRedis());
    }

    @Override
    public void close() throws Exception {
        super.close();

        if (redisAdapter != null && redisAdapter.getRedissonClient() != null) {
            redisAdapter.getRedissonClient().shutdown();
        }
    }

    /**
     * 本地缓存映射关系
     */
    private void initRelations() {
        File relationFile = getRuntimeContext().getDistributedCache().getFile(relationCacheFileName);
        String relationJson = FileUtil.readUtf8String(relationFile);
        Map<String, String> relationHdfsCache = JacksonUtil.parseObject(relationJson, new TypeReference<Map<String, String>>() {
        });
        relationLocalCache = new ConcurrentHashMap<>(relationHdfsCache.size());
        relationLocalCache.putAll(relationHdfsCache);
    }

    /**
     * delete DML过滤、ddl同步、分流
     *
     * @param value
     * @param ctx
     * @param out
     * @throws Exception
     */
    @Override
    public void processElement(RowJson value, ProcessFunction<RowJson, RowJson>.Context ctx, Collector<RowJson> out) throws Exception {
        String targetDbName = relationLocalCache.get(value.getDb());
        if (StringUtils.isBlank(targetDbName)) {
            log.warn("targetDbName.null|value={}", JacksonUtil.toJson(value));
            return;
        }

        String tableCacheKey = RedisKeyUtil.buildTableRelationKey(value.getDb(), value.getTable());
        String targetTable = relationLocalCache.get(tableCacheKey);
        if (StringUtils.isBlank(targetTable)) {
            log.warn("targetTable.null|value={}", JacksonUtil.toJson(value));
            return;
        }

        // 监听ddl
        if (value.getOp() == OpType.DDL.getType()) {
            monitorDdl(value, targetDbName, targetTable);
            return;
        }

        // 分流
        OutputTag<RowJson> outputTag = OutputTagCache.getOutputTag(targetDbName, targetTable);
        ctx.output(outputTag, value);
    }

    /**
     * 监听ddl
     *
     * @param value
     * @param targetDbName
     * @param targetTable
     */
    private void monitorDdl(RowJson value, String targetDbName, String targetTable) {
        String sourceOffset = value.getOffset();
        if (StringUtils.isBlank(sourceOffset)) {
            log.warn("syncDdl.sourceOffset.isBlank|sourceOffset={}, targetDbName={}, targetTable={}", sourceOffset, targetDbName, targetTable);
            return;
        }
        BinlogOffset binlogOffset = DdlUtil.convert(sourceOffset);
        if (binlogOffset == null || StringUtils.isBlank(binlogOffset.getFilename())) {
            log.warn("syncDdl.filename.isBlank|sourceOffset={}, targetDbName={}, targetTable={}", sourceOffset, targetDbName, targetTable);
            return;
        }

        RedissonClient redissonClient = redisAdapter.getRedissonClient();
        String ddlStateKey = RedisKeyUtil.buildDdlStateContentKey(binlogOffset.getFilename(), binlogOffset.getPosition());
        RSet<String> ddlStateSet = redissonClient.getSet(RedisKeyUtil.buildDdlStateSetKey());
        // 去重
        if (!ddlStateSet.add(ddlStateKey)) {
            log.warn("get ddl lock fail, skipped|ddl={}", value.getDdl());
            return;
        }

        try {
            // 记录ddl
            DdlDTO ddlDTO = new DdlDTO();
            ddlDTO.setDdl(value.getDdl());
            ddlDTO.setSourceDbName(value.getDb());
            ddlDTO.setSourceTableName(value.getTable());
            ddlDTO.setTargetDbName(targetDbName);
            ddlDTO.setTargetTableName(targetTable);
            ddlDTO.setMillis(System.currentTimeMillis());

            RScoredSortedSet<String> ddlSet = redissonClient.getScoredSortedSet(RedisKeyUtil.buildDdlKey(application));
            ddlSet.add(System.nanoTime(), JacksonUtil.toJson(ddlDTO));
        } catch (Exception e) {
            ddlStateSet.remove(ddlStateKey);
            throw e;
        }

        RMap<String, String> applicationCache = redissonClient.getMap(RedisKeyUtil.buildApplicationKey());
        String applicationJson = applicationCache.get(application);
        ApplicationDTO applicationDTO = JacksonUtil.parseObject(applicationJson, ApplicationDTO.class);

        String orderNo = DateUtil.format(new Date(), "yyyyMMdd_HHmmss_SSS") + "_" + RandomUtil.randomString(8);
        // 企业微信通知
        robotAdapter.noticeAfterReceiveDdl(orderNo, applicationDTO.getApplicationId(), applicationDTO.getJobId(), targetDbName, targetTable, value.getDdl(), sourceOffset);
    }

}