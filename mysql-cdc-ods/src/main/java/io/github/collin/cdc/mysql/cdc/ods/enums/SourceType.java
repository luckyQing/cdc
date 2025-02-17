package io.github.collin.cdc.mysql.cdc.ods.enums;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import io.github.collin.cdc.mysql.cdc.ods.source.FixDataMysqlSource;

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