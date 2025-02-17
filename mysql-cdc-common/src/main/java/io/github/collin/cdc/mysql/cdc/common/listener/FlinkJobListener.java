package io.github.collin.cdc.mysql.cdc.common.listener;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.io.FileUtil;
import io.github.collin.cdc.common.common.adapter.RedisAdapter;
import io.github.collin.cdc.common.dto.cache.ApplicationDTO;
import io.github.collin.cdc.common.properties.RedisProperties;
import io.github.collin.cdc.common.util.JacksonUtil;
import io.github.collin.cdc.common.util.RedisKeyUtil;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobListener;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;

import javax.annotation.Nullable;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

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
            String taskCache = JacksonUtil.toJson(applicationDTO);
            System.out.println("taskCache-->" + taskCache);
            saveJobData(applicationDTO);

            if (StringUtils.isNotBlank(applicationId)) {
                redissonClient = new RedisAdapter(redisProperties).getRedissonClient();
                RMap<String, String> applicationCache = redissonClient.getMap(RedisKeyUtil.buildApplicationKey());
                applicationCache.put(appName, taskCache);
            }
        } finally {
            if (redissonClient != null) {
                redissonClient.shutdown();
            }
        }
    }

    private void saveJobData(ApplicationDTO applicationDTO) {
        Map<String, Object> logs = new LinkedHashMap<>();
        logs.put("application", appName);
        logs.put("time:", DateUtil.formatDateTime(new Date()));
        logs.put("applicationId", applicationDTO.getApplicationId());
        logs.put("jobId", applicationDTO.getJobId());

        String dir = null;
        String os = System.getProperty("os.name").toLowerCase();
        if (os.contains("nix") || os.contains("nux") || os.contains("aix")) {
            dir = "/data/flinkjob/";
        } else {
            dir = FileUtil.getTmpDirPath();
        }

        File dirFile = new File(dir);
        if (!dirFile.exists()) {
            dirFile.mkdirs();
        }
        String fileName = "flink_job_data.txt";
        String path = dir.endsWith("/") ? dir + fileName : dir + "/" + fileName;
        FileUtil.writeLines(Arrays.asList(JacksonUtil.toJson(logs)), new File(path), StandardCharsets.UTF_8, true);
    }

    @Override
    public void onJobExecuted(@Nullable JobExecutionResult jobExecutionResult, @Nullable Throwable throwable) {
        // 无异常时为null
        if (throwable == null) {
            // 进行一些资源释放
        }
    }


}