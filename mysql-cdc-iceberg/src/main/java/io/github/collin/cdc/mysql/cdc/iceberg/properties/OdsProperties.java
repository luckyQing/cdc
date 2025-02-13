package io.github.collin.cdc.mysql.cdc.iceberg.properties;

import io.github.collin.cdc.common.properties.*;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
@ToString
public class OdsProperties implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 环境
     * @see io.github.collin.cdc.common.enums.Env
     */
    protected String env;
    /**
     * 应用名（确定后不要改动）
     */
    private String application;

    /**
     * 是否使用G1
     */
    private boolean enableG1;
    /**
     * 同步模式
     */
    private String startupMode = "INITIAL";
    /**
     * 增量发mq时的kafka server地址
     */
    private String kafkaBootstrapServers;
    /**
     * 并发数
     */
    private ParallelismProperties parallelism = new ParallelismProperties();
    /**
     * checkpoint配置
     */
    private CheckpointProperties checkpoint = new CheckpointProperties();
    /**
     * flink-maintenance服务配置
     */
    private MaintenanceProperties maintenance = new MaintenanceProperties();
    /**
     * redis配置
     */
    private RedisProperties redis = new RedisProperties();

    /**
     * 代理配置
     */
    private ProxyProperties proxy = new ProxyProperties();

    /**
     * 监控配置
     */
    private MonitorProperties monitor = new MonitorProperties();

    /**
     * 目标时区
     */
    private String targetTimeZone;

    /**
     * hdfs配置
     */
    private HdfsProperties hdfs = new HdfsProperties();

    /**
     * mysql数据源配置<数据库实例名称, 数据源配置>
     */
    private Map<String, FlinkDatasourceProperties> datasources = new HashMap<>();

}