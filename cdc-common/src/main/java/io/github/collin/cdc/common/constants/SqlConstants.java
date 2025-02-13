package io.github.collin.cdc.common.constants;

/**
 * sql常量
 *
 * @author collin
 * @date 2023-07-20
 */
public interface SqlConstants {

    String AUTO_INCREMENT = "AUTO_INCREMENT";
    String NOT_NULL = "NOT NULL";
    String PRIMARY_KEY = "PRIMARY KEY";
    String UNIQUE_KEY = "UNIQUE KEY";
    /**
     * 主键索引类型名称
     */
    String PRIMARY_KEY_INDEX_TYPE_NAME = "PRIMARY";

    /**
     * 表字段db_index
     */
    String FIELD_DB_INDEX = "db_index";
    /**
     * 表字段table_index
     */
    String FIELD_TABLE_INDEX = "table_index";

}
