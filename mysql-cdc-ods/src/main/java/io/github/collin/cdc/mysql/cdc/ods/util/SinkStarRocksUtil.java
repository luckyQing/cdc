package io.github.collin.cdc.mysql.cdc.ods.util;

import cn.hutool.core.date.DateField;
import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateTime;
import cn.hutool.core.date.DateUtil;
import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLDataType;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.statement.*;
import com.mysql.cj.MysqlType;
import com.starrocks.connector.flink.catalog.StarRocksColumn;
import com.starrocks.connector.flink.catalog.TypeUtils;
import com.starrocks.connector.flink.cdc.StarRocksOptions;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import com.starrocks.connector.flink.table.sink.StarRocksSinkSemantic;
import io.github.collin.cdc.common.constants.SchemaConstants;
import io.github.collin.cdc.mysql.cdc.common.constants.FieldConstants;
import io.github.collin.cdc.mysql.cdc.common.dto.ColumnMetaDataDTO;
import io.github.collin.cdc.mysql.cdc.ods.dto.PartitionFieldDTO;
import io.github.collin.cdc.mysql.cdc.ods.enums.MysqlType2StarRocksMapping;
import io.github.collin.cdc.mysql.cdc.ods.enums.PartitionType;
import io.github.collin.cdc.mysql.cdc.ods.properties.StarRocksProperties;
import io.github.collin.cdc.mysql.cdc.ods.starrocks.EnhanceStarRocksCatalog;
import io.github.collin.cdc.mysql.cdc.ods.starrocks.JsonRowSerializer;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class SinkStarRocksUtil {

    /**
     * 修复bug：同样长度的mysql字段，在StarRocks中可能存不下，所以需要增加一些长度
     */
    private static final int STRING_EXTR_LENGTH = 32;

    public static List<StarRocksColumn> buildStarRocksColumns(List<ColumnMetaDataDTO> columnMetaDatas, List<String> primaryKeyNames, boolean isSharding) {
        List<StarRocksColumn> columns = new ArrayList<>();
        if (isSharding) {
            columns.add(new StarRocksColumn.Builder().setColumnName(FieldConstants.DB_NAME).setDataType(TypeUtils.VARCHAR).setColumnSize(256).setColumnComment(FieldConstants.COMMENT_DB_NAME).setNullable(false).build());
            columns.add(new StarRocksColumn.Builder().setColumnName(FieldConstants.TABLE_NAME).setDataType(TypeUtils.VARCHAR).setColumnSize(256).setColumnComment(FieldConstants.COMMENT_TABLE_NAME).setNullable(false).build());
        }

        for (int i = 0; i < columnMetaDatas.size(); i++) {
            ColumnMetaDataDTO columnMetaData = columnMetaDatas.get(i);

            MysqlType mysqlType = columnMetaData.getMysqlType();
            Integer columnSize = columnMetaData.getLength();
            if (MysqlType.VARCHAR == mysqlType || MysqlType.TINYTEXT == mysqlType || MysqlType.TEXT == mysqlType
                    || MysqlType.MEDIUMTEXT == mysqlType || MysqlType.LONGTEXT == mysqlType
                    || MysqlType.BLOB == mysqlType || MysqlType.TINYBLOB == mysqlType || MysqlType.MEDIUMBLOB == mysqlType || MysqlType.LONGBLOB == mysqlType) {
                if (mysqlType.getPrecision() + STRING_EXTR_LENGTH < MysqlType.LONGBLOB.getPrecision()) {
                    columnSize = mysqlType.getPrecision().intValue() + STRING_EXTR_LENGTH;
                } else {
                    columnSize = mysqlType.getPrecision().intValue();
                }
            }

            StarRocksColumn.Builder columnBuilder = new StarRocksColumn.Builder()
                    .setColumnName(columnMetaData.getName())
                    .setDataType(MysqlType2StarRocksMapping.of(mysqlType))
                    .setColumnSize(columnSize)
                    .setDecimalDigits(columnMetaData.getDecimalDigits())
                    .setNullable(columnMetaData.isNullable())
                    .setColumnComment(columnMetaData.getComment());
            // 时间类型默认值与StarRocks不匹配，不处理
            if (mysqlType != MysqlType.DATE
                    && mysqlType != MysqlType.DATETIME
                    && mysqlType != MysqlType.TIMESTAMP
                    && mysqlType != MysqlType.TIME) {
                columnBuilder.setDefaultValue(columnMetaData.getDefaultValue());
            }

            columns.add(columnBuilder.build());
        }

        columns.add(new StarRocksColumn.Builder()
                .setColumnName(SchemaConstants.SYNC_TS)
                .setDataType(TypeUtils.BIGINT)
                .setColumnSize(20)
                .setColumnComment("同步时间")
                .setNullable(true)
                .build());

        if (primaryKeyNames.size() == 1) {
            return columns;
        }

        // Key columns must be the first few columns of the schema and the order  of the key columns must be consistent with the order of the schema
        List<StarRocksColumn> finalColumns = new ArrayList<>();

        for (String primaryKeyName : primaryKeyNames) {
            Iterator<StarRocksColumn> starRocksColumnIterator = columns.iterator();
            while (starRocksColumnIterator.hasNext()) {
                StarRocksColumn starRocksColumn = starRocksColumnIterator.next();
                if (starRocksColumn.getColumnName().equals(primaryKeyName)) {
                    finalColumns.add(starRocksColumn);
                    starRocksColumnIterator.remove();
                }
            }
        }

        // 添加剩余的
        finalColumns.addAll(columns);
        return finalColumns;
    }

    public static JsonRowSerializer buildJsonRowSerializer(String starRocksDatabase, String starRocksTable, String jdbcUrl,
                                                           String username, String password) {
        StarRocksOptions.Builder starRocksBuilder = StarRocksOptions.builder();
        starRocksBuilder.setTableIdentifier(starRocksDatabase + "." + starRocksTable)
                .setUsername(username)
                .setPassword(password)
                .setJdbcUrl(jdbcUrl)
                .setFastSchemaEvolution(true);
        return JsonRowSerializer.builder()
                .setStarRocksOptions(starRocksBuilder.build())
                .build();
    }

    public static StarRocksSinkOptions buildStarRocksSinkOptions(StarRocksProperties starRocksProperties, String starRocksDatabase, String starRocksTable) {
        StarRocksSinkOptions options = StarRocksSinkOptions.builder()
                .withProperty("jdbc-url", starRocksProperties.getJdbcUrl())
                .withProperty("load-url", starRocksProperties.getLoadUrl())
                .withProperty("database-name", starRocksDatabase)
                .withProperty("table-name", starRocksTable)
                .withProperty("username", starRocksProperties.getUsername())
                .withProperty("password", starRocksProperties.getPassword())
                .withProperty("sink.semantic", StarRocksSinkSemantic.EXACTLY_ONCE.getName())
                .withProperty("sink.version", "V2")
                .withProperty("sink.properties.format", "json")
                .withProperty("sink.properties.strip_outer_array", "true")
                .withProperty("sink.properties.ignore_json_size", "true")
                .withProperty("sink.socket.timeout-ms", "90000")
                .withProperty("sink.connect.timeout-ms", "90000")
                .withProperty("sink.wait-for-continue.timeout-ms", "90000")
                .withProperty("sink.io.thread-count", "4")
                .build();
        options.enableUpsertDelete();
        options.setSupportTransactionStreamLoad(true);

        return options;
    }

    /**
     * 获取表主键字段名
     *
     * @param columnMetaDatas
     * @param partitionField
     * @param isSharding
     * @return
     */
    public static List<String> getPrimaryKeyNames(List<ColumnMetaDataDTO> columnMetaDatas, String partitionField, boolean isSharding) {
        LinkedHashSet<String> primaryKeyNames = null;
        if (isSharding) {
            primaryKeyNames = new LinkedHashSet<>();
            primaryKeyNames.add(FieldConstants.DB_NAME);
            primaryKeyNames.add(FieldConstants.TABLE_NAME);
        }

        for (int i = 0; i < columnMetaDatas.size(); i++) {
            ColumnMetaDataDTO columnMetaData = columnMetaDatas.get(i);
            if (columnMetaData.isPrimaryKey()) {
                if (primaryKeyNames == null) {
                    primaryKeyNames = new LinkedHashSet<>();
                }
                primaryKeyNames.add(columnMetaData.getName());
            }
        }
        if (partitionField != null) {
            primaryKeyNames.add(partitionField);
        }

        return new ArrayList<>(primaryKeyNames);
    }

    public static PartitionFieldDTO getPartitionField(String targetDbNameAndTableName) {
        List<PartitionFieldDTO> partitionFieldDtos = PartitionUtil.getPartitionFields(targetDbNameAndTableName);
        if (partitionFieldDtos == null) {
            return null;
        }

        List<PartitionFieldDTO> partitionFields = partitionFieldDtos.stream()
                .filter(dto -> PartitionType.YEAR == dto.getPartitionType()
                        || PartitionType.MONTH == dto.getPartitionType()
                        || PartitionType.DAY == dto.getPartitionType())
                .collect(Collectors.toList());
        if (partitionFields == null || partitionFields.isEmpty()) {
            return null;
        }
        if (partitionFields.size() > 1) {
            throw new UnsupportedOperationException("partition field size > 1");
        }

        return partitionFields.get(0);
    }

    public static String buildPartitionSql(PartitionFieldDTO partitionFieldDto) {
        if (partitionFieldDto == null) {
            return null;
        }

        // 3.0以上版本才支持表达式分区
        //String.format("PARTITION BY date_trunc('%s', `%s`)", partitionFieldDto.getPartitionType().getValue(), partitionFieldDto.getName());
        StringBuilder partitionSql = new StringBuilder(256);
        partitionSql.append(String.format("PARTITION BY RANGE(%s)", partitionFieldDto.getName()));
        partitionSql.append("(");
        partitionSql.append(buildPartitionItemSql(partitionFieldDto.getPartitionType(), partitionFieldDto.getStartTableTime()));
        partitionSql.append(")");
        return partitionSql.toString();
    }

    private static String buildPartitionItemSql(PartitionType partitionType, Date startTableTime) {
        DateTime startTime = null;
        DateField dateField = null;
        String partitionFormat = null;
        StringBuilder partitionSql = new StringBuilder(256);
        switch (partitionType) {
            case YEAR:
                startTime = DateUtil.beginOfYear(startTableTime);
                dateField = DateField.YEAR;
                partitionFormat = DatePattern.NORM_YEAR_PATTERN;
                break;
            case MONTH:
                startTime = DateUtil.beginOfMonth(startTableTime);
                dateField = DateField.MONTH;
                partitionFormat = DatePattern.SIMPLE_MONTH_PATTERN;
                break;
            case DAY:
                startTime = DateUtil.beginOfDay(startTableTime);
                dateField = DateField.DAY_OF_MONTH;
                partitionFormat = DatePattern.PURE_DATE_PATTERN;
                break;
        }

        // 只生成近6年的
        DateTime endTime = DateUtil.offset(new Date(), DateField.YEAR, 6);
        while (!startTime.isAfter(endTime)) {
            startTime = DateUtil.offset(startTime, dateField, 1);
            partitionSql.append(String.format("PARTITION p%s VALUES [('%s'), ('%s'))", DateUtil.format(startTime, partitionFormat),
                    DateUtil.format(startTime, DatePattern.NORM_DATETIME_PATTERN),
                    DateUtil.format(DateUtil.offset(startTime, dateField, 1), DatePattern.NORM_DATETIME_PATTERN)));
            if (!startTime.isAfter(endTime)) {
                partitionSql.append(",");
            }
        }

        return partitionSql.toString();
    }

    public static Integer getNumBuckets(String targetDbNameAndTableName) {
        List<PartitionFieldDTO> partitionFieldDtos = PartitionUtil.getPartitionFields(targetDbNameAndTableName);
        if (partitionFieldDtos == null) {
            return null;
        }

        List<PartitionFieldDTO> partitionFields = partitionFieldDtos.stream()
                .filter(dto -> PartitionType.HASH == dto.getPartitionType())
                .collect(Collectors.toList());
        if (partitionFields == null || partitionFields.isEmpty()) {
            return null;
        }
        if (partitionFields.size() > 1) {
            throw new UnsupportedOperationException("partition field size > 1");
        }

        return partitionFields.get(0).getNumBuckets();
    }

    /**
     * 将ddl转化为StarRocks sql
     *
     * @param sql
     * @param dbName
     * @param tableName
     * @return
     */
    public static List<String> convertStarRocksSql(String sql, String dbName, String tableName, long timeoutSecond) {
        List<SQLStatement> statements = SQLUtils.parseStatements(sql, DbType.mysql, true);
        List<String> ddls = new ArrayList<>();
        for (SQLStatement statement : statements) {
            if (statement instanceof SQLAlterTableStatement) {
                SQLAlterTableStatement alterTableStatement = (SQLAlterTableStatement) statement;
                List<SQLAlterTableItem> sqlAlterTableItems = alterTableStatement.getItems();
                if (sqlAlterTableItems == null || sqlAlterTableItems.size() == 0) {
                    continue;
                }
                for (SQLAlterTableItem sqlAlterTableItem : sqlAlterTableItems) {
                    if (sqlAlterTableItem instanceof SQLAlterTableAddColumn) {
                        SQLAlterTableAddColumn sqlAlterTableAddColumn = (SQLAlterTableAddColumn) sqlAlterTableItem;
                        SQLColumnDefinition sqlColumnDefinition = sqlAlterTableAddColumn.getColumns().get(0);
                        String columnName = sqlColumnDefinition.getColumnName();
                        SQLDataType sqlDataType = sqlColumnDefinition.getDataType();
                        SQLCharExpr sqlExpr = (SQLCharExpr) sqlColumnDefinition.getComment();
                        String comment = sqlExpr != null ? sqlExpr.getText() : "";
                        String defaultValue = sqlColumnDefinition.getDefaultExpr() == null ? null : sqlColumnDefinition.getDefaultExpr().toString();
                        List<SQLExpr> arguments = sqlDataType.getArguments();
                        Integer columnSize = (arguments != null && arguments.size() >= 1) ? Integer.valueOf(arguments.get(0).toString()) : null;
                        Integer decimalDigits = (arguments != null && arguments.size() >= 2) ? Integer.valueOf(arguments.get(1).toString()) : null;

                        StarRocksColumn addColumn = new StarRocksColumn.Builder()
                                .setColumnName(DdlUtil.removeMysqlDelimiter(columnName))
                                .setDataType(MysqlType2StarRocksMapping.of(sqlDataType.getName()))
                                .setColumnSize(columnSize)
                                .setDecimalDigits(decimalDigits)
                                .setColumnComment(comment)
                                .setNullable(!sqlColumnDefinition.containsNotNullConstaint())
                                .setDefaultValue(defaultValue)
                                .build();
                        ddls.add(EnhanceStarRocksCatalog.buildAlterAddColumnSql(dbName, tableName, addColumn, timeoutSecond));
                    } /*else if (sqlAlterTableItem instanceof MySqlAlterTableChangeColumn) {
                    } */ else if (sqlAlterTableItem instanceof SQLAlterTableDropColumnItem) {
                        SQLAlterTableDropColumnItem sqlAlterTableDropColumnItem = (SQLAlterTableDropColumnItem) sqlAlterTableItem;
                        SQLName sqlName = sqlAlterTableDropColumnItem.getColumns().get(0);
                        ddls.add(String.format("ALTER TABLE `%s`.`%s` DROP COLUMN `%s` PROPERTIES (\"timeout\" = \"%s\")", dbName, tableName, DdlUtil.removeMysqlDelimiter(sqlName.getSimpleName()), timeoutSecond));
                    } else {
                        log.warn("ddl[{}], skip", sql);
                        System.out.println("ddl==>" + sql);
                    }
                }
            } else {
                log.warn("Non-modified structure ddl[{}], skip", sql);
            }
        }

        if (ddls.isEmpty()) {
            return Collections.emptyList();
        }

        return ddls;
    }

}