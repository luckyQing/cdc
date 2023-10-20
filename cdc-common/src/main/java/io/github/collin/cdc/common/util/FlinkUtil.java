package io.github.collin.cdc.common.util;

import io.github.collin.cdc.common.properties.CheckpointProperties;
import io.github.collin.cdc.common.properties.ParallelismProperties;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.*;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.iceberg.flink.FlinkWriteOptions;

import java.time.Duration;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * flink工具类
 *
 * @author collin
 * @date 2023-06-16
 */
public class FlinkUtil {

    /**
     * JUnitCore类名
     */
    private static final String JUNIT_CORE_CLASS_NAME = "org.junit.runner.JUnitCore";

    /**
     * 构建flink流环境
     *
     * @param zoneId
     * @param parallelismProperties
     * @param checkpointProperties
     * @return
     */
    public static StreamExecutionEnvironment buildStreamEnvironment(String zoneId, ParallelismProperties parallelismProperties, CheckpointProperties checkpointProperties) {
        return buildStreamExecutionEnvironment(null, RuntimeExecutionMode.STREAMING, zoneId, parallelismProperties, checkpointProperties, null);
    }

    /**
     * 构建flink流环境
     *
     * @param configuration
     * @param zoneId
     * @param parallelismProperties
     * @param checkpointProperties
     * @return
     */
    public static StreamExecutionEnvironment buildStreamEnvironment(Configuration configuration, String zoneId, ParallelismProperties parallelismProperties, CheckpointProperties checkpointProperties) {
        return buildStreamExecutionEnvironment(configuration, RuntimeExecutionMode.STREAMING, zoneId, parallelismProperties, checkpointProperties, null);
    }

    /**
     * 构建flink流环境
     *
     * @param zoneId
     * @param parallelismProperties
     * @param checkpointProperties
     * @param webport
     * @return
     */
    public static StreamExecutionEnvironment buildStreamEnvironment(String zoneId, ParallelismProperties parallelismProperties, CheckpointProperties checkpointProperties, Integer webport) {
        return buildStreamExecutionEnvironment(null, RuntimeExecutionMode.STREAMING, zoneId, parallelismProperties, checkpointProperties, webport);
    }

    /**
     * 构建flink批环境
     *
     * @param zoneId
     * @param parallelismProperties
     * @return
     */
    public static StreamExecutionEnvironment buildBatchEnvironment(String zoneId, ParallelismProperties parallelismProperties) {
        return buildStreamExecutionEnvironment(null, RuntimeExecutionMode.BATCH, zoneId, parallelismProperties, null, null);
    }

    /**
     * 构建flink批环境
     *
     * @param zoneId
     * @param parallelismProperties
     * @return
     */
    public static StreamExecutionEnvironment buildBatchEnvironment(Configuration configuration, String zoneId, ParallelismProperties parallelismProperties) {
        return buildStreamExecutionEnvironment(configuration, RuntimeExecutionMode.BATCH, zoneId, parallelismProperties, null, null);
    }

    /**
     * 构建flink批环境
     *
     * @param zoneId
     * @param parallelismProperties
     * @param webport
     * @return
     */
    public static StreamExecutionEnvironment buildBatchEnvironment(String zoneId, ParallelismProperties parallelismProperties, Integer webport) {
        return buildStreamExecutionEnvironment(null, RuntimeExecutionMode.BATCH, zoneId, parallelismProperties, null, webport);
    }

    /**
     * 构建flink环境环境
     *
     * @param executionMode
     * @param zoneId
     * @param parallelismProperties
     * @param checkpointProperties
     * @param webport
     * @return
     */
    private static StreamExecutionEnvironment buildStreamExecutionEnvironment(Configuration configuration, RuntimeExecutionMode executionMode, String zoneId, ParallelismProperties parallelismProperties, CheckpointProperties checkpointProperties, Integer webport) {
        TimeZone.setDefault(TimeZone.getTimeZone(ZoneId.of(zoneId)));

        Configuration copyOfConfiguration = buildConfiguration(configuration, executionMode, zoneId);
        if (webport != null) {
            copyOfConfiguration.set(RestOptions.PORT, webport);
        }

        // junit环境开启webui
        boolean isTestEnv = Arrays.stream(Thread.currentThread().getStackTrace()).anyMatch(x -> JUNIT_CORE_CLASS_NAME.equals(x.getClassName()));

        StreamExecutionEnvironment executionEnvironment = isTestEnv ? StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(copyOfConfiguration) : StreamExecutionEnvironment.getExecutionEnvironment(copyOfConfiguration);
        executionEnvironment.setRuntimeMode(executionMode);
        if (executionMode == RuntimeExecutionMode.STREAMING && checkpointProperties != null) {
            executionEnvironment.setRestartStrategy(RestartStrategies.failureRateRestart(1, Time.of(30, TimeUnit.SECONDS), Time.of(30, TimeUnit.SECONDS)));
            setCheckpointConfig(executionEnvironment.getCheckpointConfig(), checkpointProperties);
        }
        setExecutionConfig(executionMode, executionEnvironment.getConfig(), parallelismProperties);

        return executionEnvironment;
    }

    private static Configuration buildConfiguration(Configuration otherConfiguration, RuntimeExecutionMode executionMode, String zoneId) {
        Configuration configuration = otherConfiguration != null ? new Configuration(otherConfiguration) : new Configuration();
        // 任务完成后仍然可以checkpoint
        configuration.set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);
        configuration.set(CoreOptions.CHECK_LEAKED_CLASSLOADER, false);
        configuration.set(CoreOptions.CLASSLOADER_RESOLVE_ORDER, "child-first");
        configuration.set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, true);

        Set<String> jvms = new HashSet<>(2);
        // 不设置时，ddl的备注会乱码
        jvms.add("-Dfile.encoding=UTF-8");
        jvms.add("-Duser.timezone=" + zoneId);
        String jvmArgsStr = configuration.getString(CoreOptions.FLINK_JVM_OPTIONS);
        if (StringUtils.isNotBlank(jvmArgsStr)) {
            Set<String> otherJvms = Arrays.stream(jvmArgsStr.split(" ")).filter(StringUtils::isNotBlank).collect(Collectors.toSet());
            if (CollectionUtils.isNotEmpty(otherJvms)) {
                jvms.addAll(otherJvms);
            }
        }
        configuration.set(CoreOptions.FLINK_JVM_OPTIONS, StringUtils.join(jvms, " "));

        // 空闲状态(即未更新的状态)被保留的最小时间，当状态中某个 key 对应的状态未更新的时间达到阈值时，该条状态被自动清理
        if (configuration.get(ExecutionConfigOptions.IDLE_STATE_RETENTION) == null) {
            configuration.set(ExecutionConfigOptions.IDLE_STATE_RETENTION, Duration.ofMinutes(40L));
        }
        if (executionMode == RuntimeExecutionMode.BATCH) {
            configuration.set(ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "NestedLoopJoin");
            configuration.set(TableConfigOptions.LOCAL_TIME_ZONE, zoneId);
            configuration.set(FlinkWriteOptions.WRITE_PARALLELISM, 2);
        } else {
            if (configuration.get(PipelineOptions.OBJECT_REUSE) == null) {
                configuration.set(PipelineOptions.OBJECT_REUSE, false);
            }
        }

        return configuration;
    }

    private static void setExecutionConfig(RuntimeExecutionMode executionMode, ExecutionConfig executionConfig, ParallelismProperties parallelismProperties) {
        executionConfig.setUseSnapshotCompression(true);
        executionConfig.setAutoWatermarkInterval(0L);
        executionConfig.setParallelism(parallelismProperties.getExecution());
        executionConfig.setMaxParallelism(parallelismProperties.getExecution());

        if (executionMode == RuntimeExecutionMode.BATCH) {
            executionConfig.enableObjectReuse();
        }
    }

    private static void setCheckpointConfig(CheckpointConfig checkpointConfig, CheckpointProperties checkpointProperties) {
        checkpointConfig.setCheckpointInterval(checkpointProperties.getCheckpointInterval());
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.setMaxConcurrentCheckpoints(checkpointProperties.getMaxConcurrentCheckpoints());
        checkpointConfig.setMinPauseBetweenCheckpoints(checkpointProperties.getMinPauseBetweenCheckpoints());
        checkpointConfig.setCheckpointTimeout(checkpointProperties.getCheckpointTimeout());
        checkpointConfig.setTolerableCheckpointFailureNumber(checkpointProperties.getTolerableCheckpointFailureNumber());
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        checkpointConfig.enableUnalignedCheckpoints();
    }

}