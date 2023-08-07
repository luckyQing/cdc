package io.github.collin.cdc.ods.listener;

import io.github.collin.cdc.common.common.adapter.RedisAdapter;
import io.github.collin.cdc.common.dto.cache.ApplicationDTO;
import io.github.collin.cdc.common.util.JacksonUtil;
import io.github.collin.cdc.common.util.RedisKeyUtil;
import io.github.collin.cdc.ods.properties.OdsProperties;
import lombok.RequiredArgsConstructor;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobListener;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.jetbrains.annotations.Nullable;
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
    private final OdsProperties odsProperties;

    @Override
    public void onJobSubmitted(@Nullable JobClient jobClient, @Nullable Throwable throwable) {
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

            redissonClient = new RedisAdapter(odsProperties.getRedis()).getRedissonClient();
            RMap<String, String> applicationCache = redissonClient.getMap(RedisKeyUtil.buildApplicationKey());
            applicationCache.put(odsProperties.getApplication(), JacksonUtil.toJson(applicationDTO));
        } finally {
            if (redissonClient != null) {
                redissonClient.shutdown();
            }
        }
    }

    @Override
    public void onJobExecuted(@Nullable JobExecutionResult jobExecutionResult, @Nullable Throwable throwable) {
        // 无异常时为null
        if (throwable == null) {
            // 进行一些资源释放
        }
    }

}