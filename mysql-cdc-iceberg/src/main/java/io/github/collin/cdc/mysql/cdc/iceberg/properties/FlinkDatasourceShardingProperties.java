package io.github.collin.cdc.mysql.cdc.iceberg.properties;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;
import java.util.Map;

@Getter
@Setter
@ToString
public class FlinkDatasourceShardingProperties implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 原始库名
     */
    private String dbNameSource;
    /**
     * 目标库名
     */
    private String dbNameTarget;
    /**
     * 分表配置<目标表名，原始表名>
     */
    private Map<String, String> tables;

}