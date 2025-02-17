package io.github.collin.cdc.mysql.cdc.iceberg.enums;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import io.github.collin.cdc.mysql.cdc.iceberg.source.FixDataMysqlSource;

public enum SourceType {

    /**
     * @see MySqlSource
     */
    MYSQL_SOURCE,
    /**
     * 用于修复数据
     *
     * @see FixDataMysqlSource
     */
    FIX_DATA_MYSQL_SOURCE;

}