package io.github.collin.cdc.mysql.cdc.ods.util;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder;
import com.ververica.cdc.connectors.mysql.table.StartupMode;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import io.github.collin.cdc.common.constants.CdcConstants;
import io.github.collin.cdc.common.util.YamlUtil;
import io.github.collin.cdc.mysql.cdc.common.constants.FieldConstants;
import io.github.collin.cdc.mysql.cdc.common.dto.ColumnMetaDataDTO;
import io.github.collin.cdc.mysql.cdc.common.dto.RowJson;
import io.github.collin.cdc.mysql.cdc.common.properties.FlinkDatasourceProperties;
import io.github.collin.cdc.mysql.cdc.ods.cdc.AbstractMysqlCdcHandler;
import io.github.collin.cdc.mysql.cdc.ods.enums.MysqlType2IcebergMapping;
import io.github.collin.cdc.mysql.cdc.ods.enums.SinkType;
import io.github.collin.cdc.mysql.cdc.ods.properties.AbstractOdsProperties;
import io.github.collin.cdc.mysql.cdc.ods.properties.IcebergOdsProperties;
import io.github.collin.cdc.mysql.cdc.ods.properties.SinkTypeProperties;
import io.github.collin.cdc.mysql.cdc.ods.properties.StarRocksOdsProperties;
import io.github.collin.cdc.mysql.cdc.ods.schema.RowJsonDeserializationSchema;
import org.apache.iceberg.types.Types;

import java.util.*;

public class CdcUtil {
    /**
     * 启动cdc任务
     * <pre>
     *     /usr/local/flink-1.17.1/bin/flink run \
     *     -Djobmanager.memory.process.size=4096m \
     *     -Djobmanager.memory.jvm-overhead.min=256m \
     *     -Djobmanager.memory.jvm-overhead.max=256m \
     *     -Dtaskmanager.memory.process.size=18432m \
     *     -Dtaskmanager.memory.managed.size=0m \
     *     -Dtaskmanager.memory.network.min=128m \
     *     -Dtaskmanager.memory.network.max=128m \
     *     -Dtaskmanager.memory.jvm-metaspace.size=256m \
     *     -Dtaskmanager.memory.jvm-overhead.min=256m \
     *     -Dtaskmanager.memory.jvm-overhead.max=256m \
     *     -Dyarn.application.name='sync biz mysql to iceberg(ods)' \
     *     -Dstate.checkpoints.num-retained=3 \
     *     -t yarn-per-job --detached \
     *     -c io.github.collin.cdc.mysql.cdc.ods.App /data/pkg/mysql-cdc-ods-1.0.0-SNAPSHOT.jar \
     *     iceberg/prod/application-biz-test-prod.yaml
     * </pre>
     *
     * @param opArgs
     * @throws Exception
     */
    public static void createMySQLSyncDatabase(String[] opArgs) throws Exception {
        String yamlPath = null;
        if (opArgs == null || opArgs.length == 0) {
            throw new IllegalArgumentException("No yaml configuration file path specified!");
        } else {
            yamlPath = opArgs[0];
        }
        System.out.println("yamlPath=" + yamlPath);

        SinkTypeProperties sinkTypeProperties = YamlUtil.readYaml(yamlPath, SinkTypeProperties.class);
        String sinkType = sinkTypeProperties.getSinkType();


        String className = null;
        AbstractOdsProperties odsProperties = null;
        if (SinkType.STARROCKS.toString().equals(sinkType)) {
            className = "io.github.collin.cdc.mysql.cdc.ods.cdc.Mysql2StarRocksOdsHandler";
            odsProperties = YamlUtil.readYaml(yamlPath, StarRocksOdsProperties.class);
        } else {
            className = "io.github.collin.cdc.mysql.cdc.ods.cdc.Mysql2IcebergOdsHandler";
            odsProperties = YamlUtil.readYaml(yamlPath, IcebergOdsProperties.class);
        }

        AbstractMysqlCdcHandler mysqlCdcHandler = (AbstractMysqlCdcHandler) CdcUtil.class.getClassLoader()
                .loadClass(className)
                .getConstructor(odsProperties.getClass())
                .newInstance(odsProperties);

        mysqlCdcHandler.run();
    }

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
            if (columnMetaData.isPrimaryKey()) {
                nestedField = Types.NestedField.required((nestedFields.size() + 1), columnMetaData.getName(), MysqlType2IcebergMapping.of(columnMetaData.getMysqlType()), columnMetaData.getComment());
            } else {
                nestedField = Types.NestedField.optional((nestedFields.size() + 1), columnMetaData.getName(), MysqlType2IcebergMapping.of(columnMetaData.getMysqlType()), columnMetaData.getComment());
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
            if (!columnMetaData.isPrimaryKey()) {
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
                                                        int parallelism, String targetTimeZone, String startupModeStr) {
        StartupOptions startupOptions = convert(startupModeStr);

        Properties jdbcProperties = new Properties();
        jdbcProperties.put("useSSL", "false");
        jdbcProperties.put("zeroDateTimeBehavior", "convertToNull");

        int startServerId = datasourceProperties.getStartServerId();
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
                .deserializer(new RowJsonDeserializationSchema(targetTimeZone))
                .startupOptions(startupOptions)
                .serverTimeZone(datasourceProperties.getTimeZone())
                .includeSchemaChanges(true)
                .scanNewlyAddedTableEnabled(true);
        //.closeIdleReaders(true);
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

    private static StartupOptions convert(String startupModeStr) {
        StartupMode startupMode = StartupMode.valueOf(startupModeStr);
        if (startupMode == StartupMode.INITIAL) {
            return StartupOptions.initial();
        }
        if (startupMode == StartupMode.EARLIEST_OFFSET) {
            return StartupOptions.earliest();
        }
        if (startupMode == StartupMode.LATEST_OFFSET) {
            return StartupOptions.latest();
        }
        throw new IllegalArgumentException(String.format("startupMode[%s] is not unsupported", startupModeStr));
    }

}