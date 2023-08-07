package io.github.collin.cdc.migration.mysql.dto;

import io.github.collin.cdc.migration.mysql.enums.TableShardingType;
import lombok.*;

import java.io.Serializable;

/**
 * 表信息
 *
 * @author collin
 * @date 2023-07-20
 */
@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class TableDTO implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 目标库名
     */
    private String dbName;
    /**
     * 目标表名
     */
    private String tableName;
    /**
     * 是否分库分表
     */
    @SuppressWarnings("AlibabaPojoMustUsePrimitiveField")
    private boolean sharding = false;
    /**
     * 目标库分片方式
     */
    private TableShardingType shardingType;
    /**
     * uid字段名
     */
    private String fieldUidName;
    /**
     * jdbc批量提交记录大小
     */
    private int batchSize;
    /**
     * 是否空表
     */
    private boolean nodata;

}