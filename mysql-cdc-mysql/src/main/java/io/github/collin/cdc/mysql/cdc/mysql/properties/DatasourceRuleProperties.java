package io.github.collin.cdc.mysql.cdc.mysql.properties;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;
import java.util.Set;

@Getter
@Setter
@ToString
public class DatasourceRuleProperties implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 1、数据库整个表全部同步；2、只同步指定的表；3、除了指定的表，全部同步
     */
    private int type;
    /**
     * 指定的表
     */
    private Set<String> tables;
    /**
     * 分库分表配置
     */
    private DatasourceShardingProperties sharding = new DatasourceShardingProperties();

}