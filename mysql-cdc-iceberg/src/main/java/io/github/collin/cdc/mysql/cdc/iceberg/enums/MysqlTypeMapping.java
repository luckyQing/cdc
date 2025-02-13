package io.github.collin.cdc.mysql.cdc.iceberg.enums;

import com.mysql.cj.MysqlType;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * <a href="https://nightlies.apache.org/flink/flink-docs-release-1.14/zh/docs/connectors/table/jdbc/">mysql与flink类型映射</a>
 *
 * @author collin
 * @date 2023-04-04
 */
@SuppressWarnings("AlibabaEnumConstantsMustHaveComment")
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public enum MysqlTypeMapping {

    DECIMAL(MysqlType.DECIMAL, Types.DecimalType.of(38, 20)),
    DECIMAL_UNSIGNED(MysqlType.DECIMAL_UNSIGNED, Types.DecimalType.of(38, 20)),
    TINYINT(MysqlType.TINYINT, Types.IntegerType.get()),
    TINYINT_UNSIGNED(MysqlType.TINYINT_UNSIGNED, Types.IntegerType.get()),
    BOOLEAN(MysqlType.BOOLEAN, Types.BooleanType.get()),
    SMALLINT(MysqlType.SMALLINT, Types.IntegerType.get()),
    SMALLINT_UNSIGNED(MysqlType.SMALLINT_UNSIGNED, Types.IntegerType.get()),
    INT(MysqlType.INT, Types.IntegerType.get()),
    INT_UNSIGNED(MysqlType.INT_UNSIGNED, Types.IntegerType.get()),
    MEDIUMINT(MysqlType.MEDIUMINT, Types.IntegerType.get()),
    MEDIUMINT_UNSIGNED(MysqlType.MEDIUMINT_UNSIGNED, Types.IntegerType.get()),
    FLOAT(MysqlType.FLOAT, Types.FloatType.get()),
    FLOAT_UNSIGNED(MysqlType.FLOAT_UNSIGNED, Types.FloatType.get()),
    DOUBLE(MysqlType.DOUBLE, Types.DoubleType.get()),
    DOUBLE_UNSIGNED(MysqlType.DOUBLE_UNSIGNED, Types.DoubleType.get()),
    TIMESTAMP(MysqlType.TIMESTAMP, Types.TimestampType.withoutZone()),
    DATETIME(MysqlType.DATETIME, Types.TimestampType.withoutZone()),
    BIGINT(MysqlType.BIGINT, Types.LongType.get()),
    BIGINT_UNSIGNED(MysqlType.BIGINT_UNSIGNED, Types.LongType.get()),
    DATE(MysqlType.DATE, Types.DateType.get()),
    TIME(MysqlType.TIME, Types.TimeType.get()),
    VARCHAR(MysqlType.VARCHAR, Types.StringType.get()),
    BIT(MysqlType.BIT, Types.IntegerType.get()),
    TEXT(MysqlType.TEXT, Types.StringType.get()),
    TINYTEXT(MysqlType.TINYTEXT, Types.StringType.get()),
    MEDIUMTEXT(MysqlType.MEDIUMTEXT, Types.StringType.get()),
    LONGTEXT(MysqlType.LONGTEXT, Types.StringType.get()),
    CHAR(MysqlType.CHAR, Types.StringType.get()),
    BINARY(MysqlType.BINARY, Types.BinaryType.get()),
    BLOB(MysqlType.BLOB, Types.BinaryType.get()),
    TINYBLOB(MysqlType.TINYBLOB, Types.BinaryType.get()),
    MEDIUMBLOB(MysqlType.MEDIUMBLOB, Types.BinaryType.get()),
    LONGBLOB(MysqlType.LONGBLOB, Types.BinaryType.get());

    /**
     * mysql字段类型
     */
    private final MysqlType mysqlType;
    /**
     * iceberg字段类型
     */
    private final Type nestedField;

    public static Type of(MysqlType mysqlType) {
        for (MysqlTypeMapping value : MysqlTypeMapping.values()) {
            if (value.mysqlType == mysqlType) {
                return value.nestedField;
            }
        }
        return Types.StringType.get();
    }

    public static Type of(String mysqlDataType) {
        for (MysqlTypeMapping value : MysqlTypeMapping.values()) {
            if (value.mysqlType.getName().equals(mysqlDataType)) {
                return value.nestedField;
            }
        }
        return Types.StringType.get();
    }

}