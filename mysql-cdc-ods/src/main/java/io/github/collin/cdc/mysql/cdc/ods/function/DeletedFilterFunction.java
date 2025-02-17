package io.github.collin.cdc.mysql.cdc.ods.function;

import com.fasterxml.jackson.core.type.TypeReference;
import io.github.collin.cdc.common.enums.OpType;
import io.github.collin.cdc.common.properties.RedisProperties;
import io.github.collin.cdc.common.util.JacksonUtil;
import io.github.collin.cdc.common.util.RedisKeyUtil;
import io.github.collin.cdc.mysql.cdc.common.adapter.RobotAdapter;
import io.github.collin.cdc.mysql.cdc.common.dto.RowJson;
import io.github.collin.cdc.mysql.cdc.common.properties.MonitorProperties;
import io.github.collin.cdc.mysql.cdc.ods.dto.cache.PropertiesCacheDTO;
import lombok.RequiredArgsConstructor;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.redisson.api.RedissonClient;

import java.util.HashSet;
import java.util.Set;

/**
 * 物理删除过滤
 *
 * @author collin
 * @date 2023-05-26
 */
@RequiredArgsConstructor
public class DeletedFilterFunction extends RichFilterFunction<RowJson> {

    private final RedisProperties redisProperties;
    /**
     * 自定义任务名
     */
    private final String application;
    private final String instanceName;
    private transient RobotAdapter robotAdapter;
    private transient Set<String> excludeDeleteTables;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        RedissonClient redissonClient = null;
        try {
            redissonClient = new io.github.collin.cdc.common.common.adapter.RedisAdapter(redisProperties).getRedissonClient();

            String excludeDeleteTableJson = (String) redissonClient.getBucket(RedisKeyUtil.buildExcludeDeleteTableKey(application, instanceName))
                    .get();
            this.excludeDeleteTables = JacksonUtil.parseObject(excludeDeleteTableJson, new TypeReference<HashSet<String>>() {
            });

            // 读取properties
            String propertiesJson = (String) redissonClient.getBucket(RedisKeyUtil.buildPropertiesKey(application))
                    .get();
            PropertiesCacheDTO propertiesCache = JacksonUtil.parseObject(propertiesJson, PropertiesCacheDTO.class);
            MonitorProperties monitor = propertiesCache.getMonitor();
            this.robotAdapter = new RobotAdapter(propertiesCache.getProxy(), monitor.getDdl(), monitor.getDelete());
        } finally {
            if (redissonClient != null) {
                redissonClient.shutdown();
            }
        }
    }

    @Override
    public boolean filter(RowJson value) throws Exception {
        if (value.getOp() != OpType.DELETE) {
            return true;
        }
        if (excludeDeleteTables == null || excludeDeleteTables.isEmpty()) {
            return true;
        }

        //robotAdapter.noticeAfterReceiveDelete(application, value.getDb(), value.getTable(), JacksonUtil.toJson(value.getJson()));
        // 物理删除，不处理
        if (excludeDeleteTables.contains(value.getDb() + "." + value.getTable())) {
            return false;
        }
        return true;
    }

}