package io.github.collin.cdc.migration.mysql.properties;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;
import java.util.Map;

@Getter
@Setter
@ToString
public class DatasourceShardingProperties implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 原始库名
     */
    private String sourceDb;
    /**
     * 目标库名
     */
    private String targetDb;
    /**
     * 分表配置<目标表名，原始表名>
     */
    private Map<String, DatasourceShardingTableProperties> tables;

}