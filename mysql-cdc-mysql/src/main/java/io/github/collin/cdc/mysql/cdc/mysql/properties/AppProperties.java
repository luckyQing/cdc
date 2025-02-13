package io.github.collin.cdc.mysql.cdc.mysql.properties;

import io.github.collin.cdc.common.properties.*;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * mysql cdc to mysql配置属性
 *
 * @author collin
 * @date 2023-07-20
 */
@Getter
@Setter
@ToString
public class AppProperties implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 应用名（确定后不要改动）
     */
    private String application;
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
     * 代理
     */
    private ProxyProperties proxy = new ProxyProperties();
    /**
     * ddl接收监控
     */
    private RobotProperties robot = new RobotProperties();

    /**
     * jvm时区
     */
    private String jvmTimeZone;

    /**
     * mysql数据源配置<数据库实例名称, 数据源配置>
     */
    private Map<String, DatasourceCdcProperties> datasources = new HashMap<>();
    /**
     * 全局目标数据源配置
     */
    private DatasourceProperties globalTarget = new DatasourceProperties();

}