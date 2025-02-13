package io.github.collin.cdc.mysql.cdc.iceberg.dto.cache;

import io.github.collin.cdc.common.properties.ProxyProperties;
import io.github.collin.cdc.common.properties.RedisProperties;
import io.github.collin.cdc.mysql.cdc.iceberg.properties.MonitorProperties;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

@Getter
@Setter
@ToString
public class PropertiesCacheDTO implements Serializable {

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

}