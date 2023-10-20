package io.github.collin.cdc.migration.mysql.constants;

/**
 * 表字段常量
 *
 * @author collin
 * @date 2023-04-24
 */
public class FieldConstants {

    /**
     * 分库分表表字段：数据库名
     */
    public static final String DB_NAME = "db_name";
    /**
     * 分库分表表字段：表名
     */
    public static final String TABLE_NAME = "table_name";


    /**
     * database_name备注
     */
    public static final String COMMENT_DB_NAME = "数据库名";
    /**
     * table_name备注
     */
    public static final String COMMENT_TABLE_NAME = "表名";

    /**
     * 分表字段名Fuid
     */
    public static final String SHARDING_COLUMN_NAME_UID = "Fuid";

    /**
     * 分表字段名Frc_order_id
     */
    public static final String SHARDING_COLUMN_NAME_ORDER_ID = "Frc_order_id";

}