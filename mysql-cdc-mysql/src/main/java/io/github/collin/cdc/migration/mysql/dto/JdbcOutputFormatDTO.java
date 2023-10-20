package io.github.collin.cdc.migration.mysql.dto;

import io.github.collin.cdc.migration.mysql.enums.TableShardingType;
import lombok.Getter;
import lombok.Setter;
import org.apache.flink.connector.jdbc.internal.JdbcOutputFormat;

import java.util.List;
import java.util.Map;

/**
 * 每个表对应的操作对象
 *
 * @author collin
 * @date 2023-07-09
 */
@SuppressWarnings("AlibabaPojoMustOverrideToString")
@Getter
@Setter
public class JdbcOutputFormatDTO {

    private List<JdbcOutputFormat<Map<String, Object>, ?, ?>> insertOurputFormats;
    private List<JdbcOutputFormat<Map<String, Object>, ?, ?>> updateOurputFormats;
    private List<JdbcOutputFormat<Map<String, Object>, ?, ?>> deleteOurputFormats;
    /**
     * 是否有合并字段（db_index、table_index）
     */
    private boolean mergeFields;
    /**
     * 目标库分片方式
     */
    private TableShardingType shardingType;
    /**
     * 分表字段名
     */
    private String shardingFieldName;

}