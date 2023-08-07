package io.github.collin.cdc.migration.mysql.cdc;

import io.github.collin.cdc.common.common.adapter.RedisAdapter;
import io.github.collin.cdc.common.constants.CdcConstants;
import io.github.collin.cdc.common.util.FlinkUtil;
import io.github.collin.cdc.common.util.JacksonUtil;
import io.github.collin.cdc.common.util.YamlUtil;
import io.github.collin.cdc.migration.mysql.constants.DbConstants;
import io.github.collin.cdc.migration.mysql.constants.SqlConstants;
import io.github.collin.cdc.migration.mysql.dto.RowJson;
import io.github.collin.cdc.migration.mysql.dto.TableDTO;
import io.github.collin.cdc.migration.mysql.dto.cache.ConfigCacheDTO;
import io.github.collin.cdc.migration.mysql.enums.TableShardingType;
import io.github.collin.cdc.migration.mysql.function.GenericJdbcSinkAdapterFunction;
import io.github.collin.cdc.migration.mysql.listener.FlinkJobListener;
import io.github.collin.cdc.migration.mysql.util.CdcUtil;
import io.github.collin.cdc.migration.mysql.util.DbUtil;
import io.github.collin.cdc.migration.mysql.util.MigrationRedisKeyUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import io.github.collin.cdc.migration.mysql.properties.*;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;

import java.io.IOException;
import java.sql.Connection;
import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 数据迁移入口类
 *
 * @author collin
 * @date 2023-07-09
 */
@Slf4j
public class MigrationHandler {

    private final AppProperties appProperties;

    public MigrationHandler(String yamlPath) throws IOException {
        this.appProperties = YamlUtil.readYaml(yamlPath, AppProperties.class);
    }

    /**
     * mysql数据迁移入口方法
     *
     * @throws Exception
     */
    public void execute() throws Exception {
        Map<String, DatasourceCdcProperties> datasources = appProperties.getDatasources();
        if (MapUtils.isEmpty(datasources)) {
            log.warn("datasources is empty!");
            return;
        }

        RedissonClient redissonClient = null;
        StreamExecutionEnvironment env = null;
        try {
            redissonClient = new RedisAdapter(appProperties.getRedis()).getRedissonClient();
            RMap<String, String> configCache = redissonClient.getMap(MigrationRedisKeyUtil.buildConfigKey());
            configCache.clear();

            Configuration configuration = new Configuration();
            configuration.set(ExecutionConfigOptions.IDLE_STATE_RETENTION, Duration.ofMinutes(60L));
            configuration.set(PipelineOptions.OBJECT_REUSE, true);

            /*Set<String> jvms = new HashSet<>();
            jvms.add("-Dcom.sun.management.jmxremote");
            jvms.add("-Dcom.sun.management.jmxremote.port=9015");
            jvms.add("-Dcom.sun.management.jmxremote.rmi.port=1099");
            jvms.add("-Dcom.sun.management.jmxremote.ssl=false");
            jvms.add("-Dcom.sun.management.jmxremote.authenticate=false");
            configuration.set(CoreOptions.FLINK_JVM_OPTIONS, StringUtils.join(jvms, " "));*/
            env = FlinkUtil.buildStreamEnvironment(configuration, appProperties.getTargetTimeZone(), appProperties.getParallelism(), appProperties.getCheckpoint());
            for (Map.Entry<String, DatasourceCdcProperties> entry : datasources.entrySet()) {
                proccessInstance(entry.getKey(), env, appProperties, entry.getValue(), redissonClient);
            }
        } finally {
            if (redissonClient != null) {
                redissonClient.shutdown();
                redissonClient = null;
            }
        }

        env.getJobListeners().add(new FlinkJobListener(env, appProperties));

        env.execute("migration mysql");
    }

    /**
     * 处理单个数据库实例
     *
     * @param instanceName
     * @param env
     * @param appProperties
     * @param datasourceCdcProperties
     * @param redissonClient
     */
    private static void proccessInstance(String instanceName, StreamExecutionEnvironment env, AppProperties appProperties, DatasourceCdcProperties datasourceCdcProperties,
                                         RedissonClient redissonClient) {
        System.out.println("instanceName---->" + instanceName);
        String sourceTimeZone = appProperties.getSourceTimeZone();
        Map<String, DatasourceRuleProperties> details = datasourceCdcProperties.getDetails();

        // 获取所有可用的表，并缓存关系<源数据库名.源表名, 目标表名>
        Map<String, TableDTO> tableRelations = DbUtil.listAvailableTables(datasourceCdcProperties, details, sourceTimeZone);

        MysqlSourceDTO mysqlSourceDTO = createIfNotExist(tableRelations, appProperties.getGlobalTarget(), datasourceCdcProperties, sourceTimeZone);


        ConfigCacheDTO configCacheDTO = new ConfigCacheDTO();
        configCacheDTO.setTableRelations(tableRelations);
        configCacheDTO.setTimeZone(appProperties.getTargetTimeZone());
        configCacheDTO.setTargetDatasource(getTargetDatasource(datasourceCdcProperties, appProperties.getGlobalTarget()));

        RMap<String, String> configCache = redissonClient.getMap(MigrationRedisKeyUtil.buildConfigKey());
        configCache.put(MigrationRedisKeyUtil.buildConfigHashKey(instanceName), JacksonUtil.toJson(configCacheDTO));

        System.out.println(String.format("%s--->dbs=%s, tables=%s", instanceName, JacksonUtil.toJson(mysqlSourceDTO.getDatabaseList()), JacksonUtil.toJson(mysqlSourceDTO.getTableList())));
        MySqlSource<RowJson> mySqlSource = CdcUtil.buildMySqlSource(instanceName, datasourceCdcProperties.getStartServerId(), datasourceCdcProperties.getSource(),
                mysqlSourceDTO.getDatabaseList(), mysqlSourceDTO.getTableList(), sourceTimeZone, appProperties.getParallelism().getExecution());

        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), instanceName + "Source")
                .name(instanceName + "Source")
                .uid(instanceName + "Source")
                .rebalance()
                .addSink(new GenericJdbcSinkAdapterFunction(instanceName, appProperties.getRedis(), appProperties.getProxy(), appProperties.getRobot()))
                .name(instanceName + " sink")
                .uid(instanceName + " sink");

        // 释放
        tableRelations.clear();
        tableRelations = null;
    }

    private static DatasourceProperties getTargetDatasource(DatasourceCdcProperties datasourceCdcProperties, DatasourceProperties globalTarget) {
        DatasourceProperties thisTargetDatasource = datasourceCdcProperties.getTarget();
        if (thisTargetDatasource == null || StringUtils.isBlank(thisTargetDatasource.getHost())) {
            return globalTarget;
        }

        return thisTargetDatasource;
    }

    /**
     * 如果不存在库、表，则创建
     *
     * @param tableRelations
     * @param globalTarget
     * @param datasourceCdcProperties
     * @param globalTimeZone
     * @return 返回需要同步的库、表信息
     */
    private static MysqlSourceDTO createIfNotExist(Map<String, TableDTO> tableRelations, DatasourceProperties globalTarget, DatasourceCdcProperties datasourceCdcProperties,
                                                   String globalTimeZone) {
        Map<String, DatasourceRuleProperties> details = datasourceCdcProperties.getDetails();
        // MySqlSource入参databaseList
        Set<String> sourceDbNames = new HashSet<>();
        // MySqlSource入参tableList
        Set<String> tableNames = new HashSet<>();

        DatasourceProperties targetDatasource = getTargetDatasource(datasourceCdcProperties, globalTarget);

        // 当前已创建的库
        Set<String> createdDbs = new HashSet<>();
        Set<String> createdTables = new HashSet<>();
        try (Connection targetConnection = DbUtil.getConnection(targetDatasource, DbConstants.INFORMATION_SCHEMA_DBNAME, globalTimeZone)) {
            Set<String> targetDatabases = DbUtil.listDatabases(targetConnection);
            for (Map.Entry<String, DatasourceRuleProperties> entry : details.entrySet()) {
                String sourceDbName = entry.getKey();
                DatasourceRuleProperties sourceDetail = entry.getValue();

                DatasourceShardingProperties sharding = sourceDetail.getSharding();
                String dbNameSource = StringUtils.isNotBlank(sharding.getSourceDb()) ? sharding.getSourceDb() : sourceDbName;
                DatasourceProperties source = datasourceCdcProperties.getSource();
                try (Connection sourceConnection = DbUtil.getConnection(source, sourceDbName, globalTimeZone)) {
                    Set<String> tableList = DbUtil.getTables(sourceConnection, sourceDetail.getType(), sourceDetail.getTables(), sourceDetail.getSharding().getTables(), true);
                    tableList = tableList.stream().map(t -> {
                        String finalSourceDbName = StringUtils.isNotBlank(sharding.getSourceDb()) ? sharding.getSourceDb() : sourceDbName;
                        return finalSourceDbName + CdcConstants.DOT + t;
                    }).collect(Collectors.toSet());
                    tableNames.addAll(tableList);

                    // 数据库不存在则创建
                    String dbNameTarget = StringUtils.isNotBlank(sharding.getTargetDb()) ? sharding.getTargetDb() : sourceDbName;
                    if (!targetDatabases.contains(dbNameTarget) && createdDbs.add(dbNameTarget)) {
                        String createTargetDbSql = DbUtil.getCreateDatabaseSql(sourceConnection, sourceDbName);
                        createTargetDbSql = createTargetDbSql.replace(sourceDbName, dbNameTarget);
                        System.out.println("----> start create db " + dbNameTarget);
                        DbUtil.executeSql(targetConnection, createTargetDbSql);
                        System.out.println("----> end create db " + dbNameTarget);
                    }

                    // 表不存在则创建
                    for (Map.Entry<String, TableDTO> tableRelation : tableRelations.entrySet()) {
                        TableDTO targetTableDTO = tableRelation.getValue();
                        if (!targetTableDTO.getDbName().equals(dbNameTarget)) {
                            continue;
                        }

                        if (createdTables.add(targetTableDTO.getDbName() + CdcConstants.DOT + targetTableDTO.getTableName())) {
                            Set<String> targetTables = new HashSet<>();
                            if (targetTableDTO.isSharding() && targetTableDTO.getShardingType() != TableShardingType.ONE) {
                                for (int i = 0; i < targetTableDTO.getShardingType().getTableCount(); i++) {
                                    targetTables.add(targetTableDTO.getTableName() + "_" + i);
                                }
                            } else {
                                targetTables.add(targetTableDTO.getTableName());
                            }

                            targetTables = targetTables.stream()
                                    .filter(table -> !DbUtil.isTableExists(targetConnection, targetTableDTO.getDbName(), table))
                                    .collect(Collectors.toSet());
                            if (CollectionUtils.isNotEmpty(targetTables)) {
                                String tableName = tableRelation.getKey().split(CdcConstants.ESCAPE_DOT)[1];
                                String createTableTemplateSql = DbUtil.getCreateTableSql(sourceConnection, tableName);
                                for (String targetTable : targetTables) {
                                    String createTableSql = RegExUtils.replaceFirst(createTableTemplateSql, tableName, String.format("%s`.`%s", targetTableDTO.getDbName(), targetTable));
                                    // 分库分表字段，添加库、表索引
                                    createTableSql = updateCreateTableSqlForSharding(createTableSql, targetTableDTO.isSharding());
                                    System.out.println(String.format("----> start create table %s.%s", targetTableDTO.getDbName(), targetTable));
                                    DbUtil.executeSql(targetConnection, createTableSql);
                                    System.out.println(String.format("----> end create table %s.%s", targetTableDTO.getDbName(), targetTable));
                                }
                            }
                        }
                    }

                    if (!tableList.isEmpty()) {
                        sourceDbNames.add(dbNameSource);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return new MysqlSourceDTO(sourceDbNames.toArray(new String[0]), tableNames.toArray(new String[0]));
    }

    private static String updateCreateTableSqlForSharding(String createTableSql, boolean isSharding) {
        if (!isSharding) {
            return createTableSql;
        }

        if (!createTableSql.contains(SqlConstants.AUTO_INCREMENT)) {
            return createTableSql;
        }

        String[] partSqls = createTableSql.split("\n");
        StringBuilder sqls = new StringBuilder(partSqls.length);
        for (String partSql : partSqls) {
            if (partSql.contains(SqlConstants.NOT_NULL) && partSql.contains(SqlConstants.AUTO_INCREMENT)) {
                sqls.append(partSql);
                sqls.append("`db_index` VARCHAR(50) NOT NULL DEFAULT '',");
                sqls.append("`table_index` VARCHAR(50) NOT NULL DEFAULT '',");
            } else if (partSql.contains(SqlConstants.PRIMARY_KEY)) {
                // 处理索引
                int backBracketIndex = partSql.indexOf(")");
                sqls.append(partSql.substring(0, backBracketIndex) + ",`db_index`,`table_index`" + partSql.substring(backBracketIndex));
            } else {
                sqls.append(partSql);
            }
        }

        return sqls.toString();
    }

    /**
     * mysql源配置
     *
     * @author collin
     * @date 2023-04-24
     */
    @Getter
    @Setter
    @ToString
    @NoArgsConstructor
    @AllArgsConstructor
    public static class MysqlSourceDTO {

        /**
         * 源数据库列表
         */
        private String[] databaseList;

        /**
         * 源表列表
         */
        private String[] tableList;
    }

}