package io.github.collin.cdc.migration.mysql.properties;

import io.github.collin.cdc.migration.mysql.constants.FieldConstants;
import io.github.collin.cdc.migration.mysql.constants.JdbcConstants;
import io.github.collin.cdc.migration.mysql.enums.TableShardingType;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

@Getter
@Setter
@ToString
public class DatasourceShardingTableProperties implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 原始表名
     */
    private String sourceTable;
    /**
     * 目标库分片方式
     */
    private TableShardingType shardingType;
    /**
     * 分表字段名
     */
    private String shardingFieldName = FieldConstants.SHARDING_COLUMN_NAME_UID;
    /**
     * jdbc批量提交记录大小
     */
    private int batchSize = JdbcConstants.BATCH_SIZE;
    /**
     * 是否空表（空表只同步结构，不同步数据）
     */
    private boolean nodata;
    /**
     * 是否分库分表（false时用于修改库名表名的场景）
     */
    private boolean sharding = true;

}