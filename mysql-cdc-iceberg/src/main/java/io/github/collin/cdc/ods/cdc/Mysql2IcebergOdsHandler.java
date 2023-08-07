package io.github.collin.cdc.ods.cdc;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import io.github.collin.cdc.common.constants.CdcConstants;
import io.github.collin.cdc.common.enums.YamlEnv;
import io.github.collin.cdc.common.properties.HdfsProperties;
import io.github.collin.cdc.common.util.FlinkUtil;
import io.github.collin.cdc.common.util.IcebergUtil;
import io.github.collin.cdc.common.util.JacksonUtil;
import io.github.collin.cdc.common.util.YamlUtil;
import io.github.collin.cdc.ods.cache.OutputTagCache;
import io.github.collin.cdc.ods.dto.ColumnMetaDataDTO;
import io.github.collin.cdc.ods.dto.RowJson;
import io.github.collin.cdc.ods.dto.TableDTO;
import io.github.collin.cdc.ods.dto.cache.PropertiesCacheDTO;
import io.github.collin.cdc.ods.exception.PrimaryKeyStateException;
import io.github.collin.cdc.ods.function.RowJsonConvertFunction;
import io.github.collin.cdc.ods.function.SplitProcessFunction;
import io.github.collin.cdc.ods.listener.FlinkJobListener;
import io.github.collin.cdc.ods.properties.FlinkDatasourceDetailProperties;
import io.github.collin.cdc.ods.properties.FlinkDatasourceProperties;
import io.github.collin.cdc.ods.properties.FlinkDatasourceShardingProperties;
import io.github.collin.cdc.ods.properties.OdsProperties;
import io.github.collin.cdc.ods.util.CdcUtil;
import io.github.collin.cdc.ods.util.DbUtil;
import io.github.collin.cdc.ods.util.HdfsUtil;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.OutputTag;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.types.Types;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * mysql数据源同步到iceberg（ods层）
 *
 * @author collin
 * @date 2023-03-31
 */
@Slf4j
public class Mysql2IcebergOdsHandler {

    private final OdsProperties odsProperties;

    public Mysql2IcebergOdsHandler(YamlEnv yamlEnv) throws IOException {
        this.odsProperties = YamlUtil.readYaml(yamlEnv.getFileName(), OdsProperties.class);
    }

    /**
     * ods数据同步入口方法
     *
     * @throws Exception
     */
    public void run() throws Exception {
        Map<String, FlinkDatasourceProperties> datasources = odsProperties.getDatasources();
        if (MapUtils.isEmpty(datasources)) {
            log.warn("datasources is empty!");
            return;
        }

        StreamExecutionEnvironment env = FlinkUtil.buildStreamEnvironment(odsProperties.getGlobalTimeZone(), odsProperties.getParallelism(), odsProperties.getCheckpoint());

        // 将properties存hdfs中供后面使用
        PropertiesCacheDTO propertiesCacheDTO = new PropertiesCacheDTO();
        propertiesCacheDTO.setRedis(odsProperties.getRedis());
        propertiesCacheDTO.setProxy(odsProperties.getProxy());
        propertiesCacheDTO.setMonitor(odsProperties.getMonitor());

        String propertiesCacheFileName = odsProperties.getApplication() + ".json";
        HdfsUtil.overwrite(JacksonUtil.toJson(propertiesCacheDTO), odsProperties.getHdfs().getCacheDir(), propertiesCacheFileName);
        env.registerCachedFile(odsProperties.getHdfs().getCacheDir() + propertiesCacheFileName, propertiesCacheFileName);

        for (Map.Entry<String, FlinkDatasourceProperties> entry : datasources.entrySet()) {
            proccessInstance(entry.getKey(), env, odsProperties, entry.getValue(), propertiesCacheFileName);
        }

        env.getJobListeners().add(new FlinkJobListener(env, odsProperties));

        env.execute("sync mysql to iceberg(ods)");
    }

    /**
     * 处理单个数据库实例
     *
     * @param instanceName
     * @param env
     * @param odsProperties
     * @param datasourceProperties
     * @param propertiesCacheFileName
     * @throws IOException
     */
    private static void proccessInstance(String instanceName, StreamExecutionEnvironment env, OdsProperties odsProperties, FlinkDatasourceProperties datasourceProperties,
                                         String propertiesCacheFileName) throws IOException {
        HdfsProperties hdfsProperties = odsProperties.getHdfs();
        String globalTimeZone = odsProperties.getGlobalTimeZone();
        Map<String, FlinkDatasourceDetailProperties> details = datasourceProperties.getDetails();
        // 获取所有可用的数据库，并缓存关系<源数据库名, 目标数据库名>
        Map<String, String> availableDatabases = DbUtil.listAvailableDatabases(datasourceProperties, details, globalTimeZone);

        // 获取所有可用的表，并缓存关系<源数据库名.源表名, 目标表名>
        Map<String, TableDTO> availableTables = DbUtil.listAvailableTables(datasourceProperties, details, globalTimeZone);

        CatalogLoader catalogLoader = IcebergUtil.catalogConfiguration(hdfsProperties);
        HiveCatalog hiveCatalog = (HiveCatalog) catalogLoader.loadCatalog();

        MysqlSourceDTO mysqlSourceDTO = createNamespace(datasourceProperties, hiveCatalog, globalTimeZone);

        // 映射关系
        Map<String, String> relations = buildRelation(availableDatabases, availableTables);

        // 将映射关系存hdfs中供后面使用
        String cacheFileName = instanceName + ".json";
        HdfsUtil.overwrite(JacksonUtil.toJson(relations), hdfsProperties.getCacheDir(), cacheFileName);
        env.registerCachedFile(hdfsProperties.getCacheDir() + cacheFileName, cacheFileName);

        MySqlSource<RowJson> mySqlSource = CdcUtil.buildMySqlSource(datasourceProperties, mysqlSourceDTO.getDatabaseList(), mysqlSourceDTO.getTableList(),
                globalTimeZone, odsProperties.getParallelism().getExecution());

        SingleOutputStreamOperator<RowJson> dataStream = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), instanceName + "Source")
                .uid(instanceName + "Source").shuffle()
                //.filter(new DeletedFilterFunction(odsProperties.getApplication(), propertiesCacheFileName))
                //.uid(String.format("%s filter delete ops", instanceName))
                .process(new SplitProcessFunction(odsProperties.getApplication(), cacheFileName, propertiesCacheFileName))
                .uid(String.format("%s monitor ddl && output by tag", instanceName));

        String url = DbUtil.buildUrl(datasourceProperties.getHost(), datasourceProperties.getPort(), datasourceProperties.getOneDatabaseName(), globalTimeZone);

        Set<String> proccessedTables = new HashSet<>();
        try (Connection connection = DbUtil.getConnection(datasourceProperties.getUsername(), datasourceProperties.getPassword(), url)) {
            for (Map.Entry<String, TableDTO> entry : availableTables.entrySet()) {
                TableDTO tableDTO = entry.getValue();
                // 已处理的不再处理，否则多个线程操作输出流会报错
                String targetDbNameAndTableName = tableDTO.getDbName() + CdcConstants.DOT + tableDTO.getTableName();
                if (!proccessedTables.add(targetDbNameAndTableName)) {
                    continue;
                }

                String targetDbName = CdcConstants.NAMESPACE_ODS_PRE + tableDTO.getDbName();
                Namespace namespace = Namespace.of(targetDbName);
                TableIdentifier identifier = TableIdentifier.of(namespace, tableDTO.getTableName());
                Table table = IcebergUtil.getExistTable(hdfsProperties, targetDbName, tableDTO.getTableName());
                if (table == null) {
                    String[] strs = entry.getKey().split(CdcConstants.ESCAPE_DOT);
                    String sourceDbName = strs[0];
                    String sourceTableName = strs[1];
                    List<ColumnMetaDataDTO> columnMetaDatas = null;
                    try {
                        columnMetaDatas = DbUtil.getTableColumnMetaDatas(connection, sourceDbName, sourceTableName);
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }

                    List<Types.NestedField> nestedFields = CdcUtil.getColumns(columnMetaDatas, tableDTO.isSharding());
                    Set<Integer> primaryKeyNames = CdcUtil.getPrimaryKeyNames(columnMetaDatas, tableDTO.isSharding());

                    System.out.println(String.format("---->start create table[%s]", identifier));
                    table = IcebergUtil.createTable(new Schema(nestedFields, primaryKeyNames), hiveCatalog, identifier);
                    System.out.println(String.format("---->end create table[%s]", identifier));
                }
                Schema schema = table.schema();
                Set<String> identifierFieldNames = schema.identifierFieldNames();
                if (identifierFieldNames == null || identifierFieldNames.isEmpty()) {
                    throw new PrimaryKeyStateException(String.format("[%s.%s] primaryKeyNames is null", tableDTO.getDbName(), tableDTO.getTableName()));
                }

                OutputTag<RowJson> outputTag = OutputTagCache.getOutputTag(tableDTO.getDbName(), tableDTO.getTableName());
                DataStream<RowData> inputStream = dataStream.getSideOutput(outputTag)
                        .flatMap(new RowJsonConvertFunction(FlinkSchemaUtil.convert(schema), tableDTO.isSharding()))
                        .name(targetDbNameAndTableName + " convert").shuffle();

                TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, identifier);
                // 写iceberg
                FlinkSink.forRowData(inputStream)
                        .table(table)
                        .tableLoader(tableLoader)
                        .equalityFieldColumns(new ArrayList<>(identifierFieldNames))
                        .writeParallelism(odsProperties.getParallelism().getWrite())
                        .upsert(true)
                        .uidPrefix(targetDbNameAndTableName)
                        .append();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // 释放
        proccessedTables.clear();
        proccessedTables = null;
    }

    /**
     * 创建namespace
     *
     * @param datasourceProperties
     * @param hiveCatalog
     * @param globalTimeZone
     * @return 返回需要同步的库表信息
     */
    private static MysqlSourceDTO createNamespace(FlinkDatasourceProperties datasourceProperties, HiveCatalog hiveCatalog, String globalTimeZone) {
        Map<String, FlinkDatasourceDetailProperties> details = datasourceProperties.getDetails();
        // MySqlSource入参databaseList
        Set<String> dbNames = new HashSet<>();
        // MySqlSource入参tableList
        Set<String> tableNames = new HashSet<>();
        for (Map.Entry<String, FlinkDatasourceDetailProperties> entry : details.entrySet()) {
            String dbName = entry.getKey();
            FlinkDatasourceDetailProperties sourceDetail = entry.getValue();

            FlinkDatasourceShardingProperties sharding = sourceDetail.getSharding();

            String url = DbUtil.buildUrl(datasourceProperties.getHost(), datasourceProperties.getPort(), dbName, globalTimeZone);
            try (Connection connection = DbUtil.getConnection(datasourceProperties.getUsername(), datasourceProperties.getPassword(), url)) {
                Set<String> tableList = DbUtil.getTables(connection, sourceDetail.getType(), sourceDetail.getTables(), sourceDetail.getSharding().getTables());
                tableList = tableList.stream().map(t -> {
                    String sourceDbName = StringUtils.isNotBlank(sharding.getDbNameSource()) ? sharding.getDbNameSource() : dbName;
                    return sourceDbName + CdcConstants.DOT + t;
                }).collect(Collectors.toSet());
                tableNames.addAll(tableList);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            // namespace不存在则创建
            String dbNameTarget = StringUtils.isNotBlank(sharding.getDbNameTarget()) ? sharding.getDbNameTarget() : dbName;
            Namespace namespace = Namespace.of(CdcConstants.NAMESPACE_ODS_PRE + dbNameTarget);
            if (!hiveCatalog.namespaceExists(namespace)) {
                System.out.println(String.format("---->start create namespace[%s]", namespace.toString()));
                hiveCatalog.createNamespace(namespace);
                System.out.println(String.format("---->end create namespace[%s]", namespace.toString()));
            }
            String dbNameSource = StringUtils.isNotBlank(sharding.getDbNameSource()) ? sharding.getDbNameSource() : dbName;
            dbNames.add(dbNameSource);
        }

        return new MysqlSourceDTO(dbNames.toArray(new String[0]), tableNames.toArray(new String[0]));
    }

    /**
     * 映射关系
     * <p>
     * 数据库：<源数据库名, 目标数据库名><br>
     * 表：<源数据库名.原表名, 目标表名>
     *
     * @param availableDatabases
     * @param availableTables
     * @return
     */
    private static Map<String, String> buildRelation(Map<String, String> availableDatabases, Map<String, TableDTO> availableTables) {
        Map<String, String> relations = new HashMap<>(availableDatabases.size() + availableTables.size());
        relations.putAll(availableDatabases);
        for (Map.Entry<String, TableDTO> entry : availableTables.entrySet()) {
            relations.put(entry.getKey(), entry.getValue().getTableName());
        }

        return relations;
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