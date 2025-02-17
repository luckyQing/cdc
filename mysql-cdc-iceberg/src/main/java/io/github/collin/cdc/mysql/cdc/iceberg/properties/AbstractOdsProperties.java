package io.github.collin.cdc.mysql.cdc.iceberg.properties;

import io.github.collin.cdc.common.properties.CheckpointProperties;
import io.github.collin.cdc.common.properties.ParallelismProperties;
import io.github.collin.cdc.common.properties.ProxyProperties;
import io.github.collin.cdc.common.properties.RedisProperties;
import io.github.collin.cdc.mysql.cdc.common.properties.FlinkDatasourceProperties;
import io.github.collin.cdc.mysql.cdc.common.properties.MonitorProperties;
import io.github.collin.cdc.mysql.cdc.iceberg.enums.SourceType;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
@ToString
public class AbstractOdsProperties extends SinkTypeProperties {

    private static final long serialVersionUID = 1L;

    /**
     * 环境
     *
     * @see io.github.collin.cdc.common.enums.Env
     */
    protected String env;
    /**
     * 数据源类型（默认：MYSQL_SOURCE）
     *
     * @see SourceType
     */
    private String sourceType;
    /**
     * 修复数据查询sql
     * <pre>select * from %s where create_time between '2024-12-18 09:00:00' and '2024-12-18 14:00:00'</pre>
     */
    private String fixSql;
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
     * mysql数据源配置<数据库实例名称, 数据源配置>
     */
    private Map<String, FlinkDatasourceProperties> datasources = new HashMap<>();

}