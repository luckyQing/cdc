package io.github.collin.cdc.migration.mysql.util;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import io.github.collin.cdc.common.constants.CdcConstants;
import io.github.collin.cdc.migration.mysql.deserialization.RowJsonDeserializationSchema;
import io.github.collin.cdc.migration.mysql.dto.RowJson;
import io.github.collin.cdc.migration.mysql.properties.DatasourceProperties;

import java.util.Properties;
import java.util.TimeZone;

/**
 * cdc工具类
 *
 * @author collin
 * @date 2023-07-20
 */
public class CdcUtil {

    public static MySqlSource<RowJson> buildMySqlSource(String instanceName, int startServerId, Long startupTimestampMillis, DatasourceProperties datasourceProperties, String[] dbNames, String[] tableNames,
                                                        String sourceTimeZone, String targetTimeZone, int parallelism) {
        Properties jdbcProperties = new Properties();
        jdbcProperties.put("useSSL", "false");
        jdbcProperties.put("zeroDateTimeBehavior", "convertToNull");

        int endServerId = startServerId + parallelism - 1;
        int connectionPoolSize = caculateConnectionPoolSize(parallelism);
        System.out.println(String.format("%s-->connectionPoolSize=%s", instanceName, connectionPoolSize));


        TimeZone target = TimeZone.getTimeZone(targetTimeZone);
        TimeZone source = TimeZone.getTimeZone(sourceTimeZone);
        int zoneDif = target.getRawOffset()-source.getRawOffset();

        MySqlSourceBuilder<RowJson> mySqlSourceBuilder = MySqlSource.<RowJson>builder()
                .hostname(datasourceProperties.getHost())
                .port(datasourceProperties.getPort())
                .connectionPoolSize(connectionPoolSize)
                .jdbcProperties(jdbcProperties)
                .databaseList(dbNames)
                .tableList(tableNames)
                .username(datasourceProperties.getUsername())
                .password(datasourceProperties.getPassword())
                .serverId(String.format("%d-%d", startServerId, endServerId))
                .deserializer(new RowJsonDeserializationSchema(false, targetTimeZone, zoneDif))
                //.splitSize(4096)
                .fetchSize(1024)
                .startupOptions(startupTimestampMillis == null ? StartupOptions.initial() : StartupOptions.timestamp(startupTimestampMillis))
                .serverTimeZone(sourceTimeZone)
                .includeSchemaChanges(false)
                .scanNewlyAddedTableEnabled(true);
        //.closeIdleReaders(true);
        return mySqlSourceBuilder.build();
    }

    /**
     * 计算数据库连接池的连接数
     *
     * @param parallelism
     * @return
     */
    private static int caculateConnectionPoolSize(int parallelism) {
        int poolSize = parallelism * 4;
        if (poolSize > CdcConstants.DB_CONNECTION_POOL_MAX_SIZE) {
            return CdcConstants.DB_CONNECTION_POOL_MAX_SIZE;
        }

        return poolSize;
    }

    /*private static int caculateConnectionPoolSize(String[] tableNames) {
        if (tableNames.length > CdcConstants.DB_CONNECTION_POOL_MAX_SIZE) {
            return CdcConstants.DB_CONNECTION_POOL_MAX_SIZE;
        }

        if (tableNames.length < CdcConstants.DB_CONNECTION_POOL_MIN_SIZE) {
            return CdcConstants.DB_CONNECTION_POOL_MIN_SIZE;
        }

        return tableNames.length;
    }*/

}