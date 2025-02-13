package io.github.collin.cdc.mysql.cdc.common.listener;

import io.github.collin.cdc.common.common.adapter.RedisAdapter;
import io.github.collin.cdc.common.dto.cache.ApplicationDTO;
import io.github.collin.cdc.common.properties.RedisProperties;
import io.github.collin.cdc.common.util.JacksonUtil;
import io.github.collin.cdc.common.util.RedisKeyUtil;
import lombok.RequiredArgsConstructor;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobListener;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;

/**
 * flink 任务监听
 *
 * @author collin
 * @date 2023-05-31
 */
@RequiredArgsConstructor
public class FlinkJobListener implements JobListener {

    private final StreamExecutionEnvironment env;
    /**
     * 应用名
     */
    private final String appName;
    private final RedisProperties redisProperties;

    @Override
    public void onJobSubmitted(JobClient jobClient, Throwable throwable) {
        // applicationId 配置项
        ConfigOption<String> applicationIdConfigOption = ConfigOptions.key("yarn.application.id")
                .stringType()
                .noDefaultValue();
        String applicationId = env.getConfiguration().get(applicationIdConfigOption);

        RedissonClient redissonClient = null;
        try {
            ApplicationDTO applicationDTO = new ApplicationDTO();
            applicationDTO.setApplicationId(applicationId);
            applicationDTO.setJobId(jobClient.getJobID().toString());

            redissonClient = new RedisAdapter(redisProperties).getRedissonClient();
            RMap<String, String> applicationCache = redissonClient.getMap(RedisKeyUtil.buildApplicationKey());
            applicationCache.put(appName, JacksonUtil.toJson(applicationDTO));
        } finally {
            if (redissonClient != null) {
                redissonClient.shutdown();
            }
        }
    }

    @Override
    public void onJobExecuted(JobExecutionResult jobExecutionResult, Throwable throwable) {
        // 无异常时为null
        if (throwable == null) {
            // 进行一些资源释放
        }
    }

}