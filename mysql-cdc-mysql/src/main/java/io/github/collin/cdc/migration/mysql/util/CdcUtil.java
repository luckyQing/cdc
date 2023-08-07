package io.github.collin.cdc.migration.mysql.util;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import io.github.collin.cdc.common.constants.CdcConstants;
import io.github.collin.cdc.migration.mysql.deserialization.RowJsonDeserializationSchema;
import io.github.collin.cdc.migration.mysql.dto.RowJson;
import io.github.collin.cdc.migration.mysql.properties.DatasourceProperties;

import java.util.Properties;

/**
 * cdc工具类
 *
 * @author collin
 * @date 2023-07-20
 */
public class CdcUtil {

    public static MySqlSource<RowJson> buildMySqlSource(String instanceName, int startServerId, DatasourceProperties datasourceProperties, String[] dbNames, String[] tableNames,
                                                        String globalTimeZone, int parallelism) {
        Properties jdbcProperties = new Properties();
        jdbcProperties.put("useSSL", "false");

        int endServerId = startServerId + parallelism - 1;
        int connectionPoolSize = caculateConnectionPoolSize(parallelism);
        System.out.println(String.format("%s-->connectionPoolSize=%s", instanceName, connectionPoolSize));
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
                .deserializer(new RowJsonDeserializationSchema(false, globalTimeZone))
                //.splitSize(4096)
                .fetchSize(1024)
                .startupOptions(StartupOptions.initial())
                .serverTimeZone(globalTimeZone)
                .includeSchemaChanges(false)
                .scanNewlyAddedTableEnabled(true)
                .closeIdleReaders(true);
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