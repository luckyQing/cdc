package io.github.collin.cdc.ods.util;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import io.github.collin.cdc.common.constants.CdcConstants;
import io.github.collin.cdc.common.util.IcebergUtil;
import io.github.collin.cdc.ods.constants.FieldConstants;
import io.github.collin.cdc.ods.dto.ColumnMetaDataDTO;
import io.github.collin.cdc.ods.dto.RowJson;
import io.github.collin.cdc.ods.enums.MysqlTypeMapping;
import io.github.collin.cdc.ods.properties.FlinkDatasourceProperties;
import io.github.collin.cdc.ods.schema.RowJsonDeserializationSchema;
import org.apache.iceberg.types.Types;

import java.util.*;

public class CdcUtil {

    /**
     * 获取表字段
     *
     * @param columnMetaDatas
     * @param isSharding
     * @return
     */
    public static List<Types.NestedField> getColumns(List<ColumnMetaDataDTO> columnMetaDatas, boolean isSharding) {
        List<Types.NestedField> nestedFields = new ArrayList<>(columnMetaDatas.size());
        if (isSharding) {
            Types.NestedField dbNestedField = Types.NestedField.required(1, FieldConstants.DB_NAME, Types.StringType.get(), FieldConstants.COMMENT_DB_NAME);
            nestedFields.add(dbNestedField);

            Types.NestedField tableNestedField = Types.NestedField.required(2, FieldConstants.TABLE_NAME, Types.StringType.get(), FieldConstants.COMMENT_TABLE_NAME);
            nestedFields.add(tableNestedField);
        }

        for (int i = 0; i < columnMetaDatas.size(); i++) {
            ColumnMetaDataDTO columnMetaData = columnMetaDatas.get(i);
            Types.NestedField nestedField = null;
            if (columnMetaData.getPrimaryKey()) {
                nestedField = Types.NestedField.required((nestedFields.size() + 1), columnMetaData.getName(), MysqlTypeMapping.of(columnMetaData.getMysqlType()), columnMetaData.getComment());
            } else {
                nestedField = Types.NestedField.optional((nestedFields.size() + 1), columnMetaData.getName(), MysqlTypeMapping.of(columnMetaData.getMysqlType()), columnMetaData.getComment());
            }
            nestedFields.add(nestedField);
        }

        IcebergUtil.addSyncTsColumn(nestedFields);
        return nestedFields;
    }

    /**
     * 获取表主键字段名
     *
     * @param columnMetaDatas
     * @return
     */
    public static Set<Integer> getPrimaryKeyNames(List<ColumnMetaDataDTO> columnMetaDatas, boolean isSharding) {
        Set<Integer> primaryKeyNames = null;
        int startIndex = 0;
        if (isSharding) {
            primaryKeyNames = new LinkedHashSet<>();
            primaryKeyNames.add(1);
            primaryKeyNames.add(2);
            startIndex = 2;
        }

        for (int i = 0; i < columnMetaDatas.size(); i++) {
            ColumnMetaDataDTO columnMetaData = columnMetaDatas.get(i);
            if (!columnMetaData.getPrimaryKey()) {
                continue;
            }

            if (primaryKeyNames == null) {
                primaryKeyNames = new LinkedHashSet<>();
            }
            primaryKeyNames.add(startIndex + i + 1);
        }

        return primaryKeyNames;
    }

    public static MySqlSource<RowJson> buildMySqlSource(FlinkDatasourceProperties datasourceProperties, String[] dbNames, String[] tableNames,
                                                        String globalTimeZone, int parallelism) {
        Properties jdbcProperties = new Properties();
        jdbcProperties.put("useSSL", "false");

        int startServerId = 6001;
        int endServerId = startServerId + parallelism - 1;
        MySqlSourceBuilder<RowJson> mySqlSourceBuilder = MySqlSource.<RowJson>builder()
                .hostname(datasourceProperties.getHost())
                .port(datasourceProperties.getPort())
                .connectionPoolSize(caculateConnectionPoolSize(tableNames))
                .jdbcProperties(jdbcProperties)
                .databaseList(dbNames)
                .tableList(tableNames)
                .username(datasourceProperties.getUsername())
                .password(datasourceProperties.getPassword())
                .serverId(String.format("%d-%d", startServerId, endServerId))
                .deserializer(new RowJsonDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .serverTimeZone(globalTimeZone)
                .includeSchemaChanges(true)
                .scanNewlyAddedTableEnabled(true)
                .closeIdleReaders(true);
        return mySqlSourceBuilder.build();
    }

    private static int caculateConnectionPoolSize(String[] tableNames) {
        if (tableNames.length > CdcConstants.DB_CONNECTION_POOL_MAX_SIZE) {
            return CdcConstants.DB_CONNECTION_POOL_MAX_SIZE;
        }

        if (tableNames.length < CdcConstants.DB_CONNECTION_POOL_MIN_SIZE) {
            return CdcConstants.DB_CONNECTION_POOL_MIN_SIZE;
        }

        return tableNames.length;
    }

}