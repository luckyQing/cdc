package io.github.collin.cdc.mysql.cdc.ods.enums;

import com.mysql.cj.MysqlType;
import com.starrocks.connector.flink.catalog.TypeUtils;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;

/**
 * <a href="https://docs.ninedata.cloud/replication/mysql_to_others/mysql_to_starrocks/">mysql与StarRocks类型映射</a>
 *
 * @author collin
 * @date 2024-11-29
 */
@SuppressWarnings("AlibabaEnumConstantsMustHaveComment")
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public enum MysqlType2StarRocksMapping {

    DECIMAL(MysqlType.DECIMAL, TypeUtils.DECIMAL),
    DECIMAL_UNSIGNED(MysqlType.DECIMAL_UNSIGNED, TypeUtils.DECIMAL),
    TINYINT(MysqlType.TINYINT, TypeUtils.TINYINT),
    TINYINT_UNSIGNED(MysqlType.TINYINT_UNSIGNED, TypeUtils.TINYINT),
    BOOLEAN(MysqlType.BOOLEAN, TypeUtils.BOOLEAN),
    SMALLINT(MysqlType.SMALLINT, TypeUtils.SMALLINT),
    SMALLINT_UNSIGNED(MysqlType.SMALLINT_UNSIGNED, TypeUtils.SMALLINT),
    INT(MysqlType.INT, TypeUtils.INT),
    INT_UNSIGNED(MysqlType.INT_UNSIGNED, TypeUtils.INT),
    MEDIUMINT(MysqlType.MEDIUMINT, TypeUtils.BIGINT),
    MEDIUMINT_UNSIGNED(MysqlType.MEDIUMINT_UNSIGNED, TypeUtils.BIGINT),
    FLOAT(MysqlType.FLOAT, TypeUtils.FLOAT),
    FLOAT_UNSIGNED(MysqlType.FLOAT_UNSIGNED, TypeUtils.FLOAT),
    DOUBLE(MysqlType.DOUBLE, TypeUtils.DOUBLE),
    DOUBLE_UNSIGNED(MysqlType.DOUBLE_UNSIGNED, TypeUtils.DOUBLE),
    TIMESTAMP(MysqlType.TIMESTAMP, TypeUtils.DATETIME),
    DATETIME(MysqlType.DATETIME, TypeUtils.DATETIME),
    BIGINT(MysqlType.BIGINT, TypeUtils.BIGINT),
    BIGINT_UNSIGNED(MysqlType.BIGINT_UNSIGNED, TypeUtils.BIGINT),
    //BIGINT_UNSIGNED(MysqlType.BIGINT_UNSIGNED, TypeUtils.LARGEINT),
    DATE(MysqlType.DATE, TypeUtils.DATE),
    TIME(MysqlType.TIME, TypeUtils.VARCHAR),
    VARCHAR(MysqlType.VARCHAR, TypeUtils.VARCHAR),
    BIT(MysqlType.BIT, TypeUtils.SMALLINT),
    TEXT(MysqlType.TEXT, TypeUtils.STRING),
    TINYTEXT(MysqlType.TINYTEXT, TypeUtils.STRING),
    MEDIUMTEXT(MysqlType.MEDIUMTEXT, TypeUtils.STRING),
    LONGTEXT(MysqlType.LONGTEXT, TypeUtils.STRING),
    CHAR(MysqlType.CHAR, TypeUtils.CHAR),
    BINARY(MysqlType.BINARY, "BINARY"),
    BLOB(MysqlType.BLOB, TypeUtils.STRING),
    TINYBLOB(MysqlType.TINYBLOB, TypeUtils.STRING),
    MEDIUMBLOB(MysqlType.MEDIUMBLOB, TypeUtils.STRING),
    LONGBLOB(MysqlType.LONGBLOB, TypeUtils.STRING);

    /**
     * mysql字段类型
     */
    private final MysqlType mysqlType;
    /**
     * iceberg字段类型
     */
    private final String nestedField;

    public static String of(MysqlType mysqlType) {
        for (MysqlType2StarRocksMapping value : MysqlType2StarRocksMapping.values()) {
            if (value.mysqlType == mysqlType) {
                return value.nestedField;
            }
        }
        throw new UnsupportedOperationException(mysqlType.toString());
    }

    public static String of(String mysqlDataType) {
        for (MysqlType2StarRocksMapping value : MysqlType2StarRocksMapping.values()) {
            if (value.mysqlType.getName().equalsIgnoreCase(mysqlDataType)) {
                return value.nestedField;
            }
        }
        throw new UnsupportedOperationException(mysqlDataType);
    }

}