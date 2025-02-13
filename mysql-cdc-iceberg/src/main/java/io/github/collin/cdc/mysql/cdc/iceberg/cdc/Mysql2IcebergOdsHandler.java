package io.github.collin.cdc.mysql.cdc.iceberg.cdc;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import io.github.collin.cdc.common.constants.CdcConstants;
import io.github.collin.cdc.common.enums.Namespaces;
import io.github.collin.cdc.common.properties.HdfsProperties;
import io.github.collin.cdc.common.util.*;
import io.github.collin.cdc.mysql.cdc.common.dto.ColumnMetaDataDTO;
import io.github.collin.cdc.mysql.cdc.common.dto.RowJson;
import io.github.collin.cdc.mysql.cdc.common.dto.TableDTO;
import io.github.collin.cdc.mysql.cdc.common.listener.FlinkJobListener;
import io.github.collin.cdc.mysql.cdc.common.properties.FlinkDatasourceDetailProperties;
import io.github.collin.cdc.mysql.cdc.common.properties.FlinkDatasourceProperties;
import io.github.collin.cdc.mysql.cdc.common.properties.FlinkDatasourceShardingProperties;
import io.github.collin.cdc.mysql.cdc.common.util.DbCommonUtil;
import io.github.collin.cdc.mysql.cdc.iceberg.cache.OutputTagCache;
import io.github.collin.cdc.mysql.cdc.iceberg.dto.cache.PropertiesCacheDTO;
import io.github.collin.cdc.mysql.cdc.iceberg.exception.PrimaryKeyStateException;
import io.github.collin.cdc.mysql.cdc.iceberg.function.RowJsonConvertFunction;
import io.github.collin.cdc.mysql.cdc.iceberg.function.SplitMQProcessFunction;
import io.github.collin.cdc.mysql.cdc.iceberg.function.SplitTableProcessFunction;
import io.github.collin.cdc.mysql.cdc.iceberg.properties.OdsProperties;
import io.github.collin.cdc.mysql.cdc.iceberg.util.CdcUtil;
import io.github.collin.cdc.mysql.cdc.iceberg.util.HdfsUtil;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.admin.AdminClient;
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.admin.NewTopic;
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
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

    public Mysql2IcebergOdsHandler(String yamlName) throws IOException {
        this.odsProperties = YamlUtil.readYaml(yamlName, OdsProperties.class);
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

        Configuration configuration = null;
        if (odsProperties.isEnableG1()) {
            Set<String> jvms = new HashSet<>(1);
            // 使用G1收集器
            jvms.add("-XX:+UseG1GC");
            configuration = new Configuration();
            configuration.set(CoreOptions.FLINK_JVM_OPTIONS, StringUtils.join(jvms, " "));
        }
        StreamExecutionEnvironment env = FlinkUtil.buildStreamEnvironment(configuration, odsProperties.getTargetTimeZone(), odsProperties.getParallelism(), odsProperties.getCheckpoint());

        // 将properties存hdfs中供后面使用
        PropertiesCacheDTO propertiesCacheDTO = new PropertiesCacheDTO();
        propertiesCacheDTO.setRedis(odsProperties.getRedis());
        propertiesCacheDTO.setProxy(odsProperties.getProxy());
        propertiesCacheDTO.setMonitor(odsProperties.getMonitor());

        String propertiesCacheFileName = odsProperties.getApplication() + ".json";
        HdfsUtil.overwrite(JacksonUtil.toJson(propertiesCacheDTO), odsProperties.getHdfs().getCacheDir(), propertiesCacheFileName);
        env.registerCachedFile(odsProperties.getHdfs().getCacheDir() + propertiesCacheFileName, propertiesCacheFileName);

        AdminClient adminClient = null;
        Set<String> topics = null;
        if (StringUtils.isNotBlank(odsProperties.getKafkaBootstrapServers())) {
            Properties adminProperties = new Properties();
            adminProperties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, odsProperties.getKafkaBootstrapServers());
            adminClient = AdminClient.create(adminProperties);
            topics = adminClient.listTopics().names().get();
        }

        for (Map.Entry<String, FlinkDatasourceProperties> entry : datasources.entrySet()) {
            proccessInstance(entry.getKey(), env, odsProperties, entry.getValue(), propertiesCacheFileName, adminClient, topics);
        }

        env.getJobListeners().add(new FlinkJobListener(env, odsProperties.getApplication(), odsProperties.getRedis()));

        // 释放
        if (adminClient != null) {
            adminClient.close();
        }
        if (topics != null) {
            topics.clear();
            topics = null;
        }

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
                                         String propertiesCacheFileName, AdminClient adminClient, Set<String> topics) throws IOException {
        HdfsProperties hdfsProperties = odsProperties.getHdfs();
        Map<String, FlinkDatasourceDetailProperties> details = datasourceProperties.getDetails();
        // 获取所有可用的数据库，并缓存关系<源数据库名, 目标数据库名>
        Map<String, String> availableDatabases = DbCommonUtil.listAvailableDatabases(datasourceProperties, details);

        // 获取所有可用的表，并缓存关系<源数据库名.源表名, 目标表名>
        Map<String, TableDTO> availableTables = DbCommonUtil.listAvailableTables(datasourceProperties, details);

        CatalogLoader catalogLoader = IcebergUtil.catalogConfiguration(hdfsProperties);
        HiveCatalog hiveCatalog = (HiveCatalog) catalogLoader.loadCatalog();

        MysqlSourceDTO mysqlSourceDTO = createNamespace(datasourceProperties, hiveCatalog, odsProperties.getEnv());

        // 映射关系
        Map<String, String> relations = buildRelation(availableDatabases, availableTables);

        // 将映射关系存hdfs中供后面使用
        String cacheFileName = instanceName + ".json";
        HdfsUtil.overwrite(JacksonUtil.toJson(relations), hdfsProperties.getCacheDir(), cacheFileName);
        env.registerCachedFile(hdfsProperties.getCacheDir() + cacheFileName, cacheFileName);

        System.out.println("source:"+JacksonUtil.toJson(mysqlSourceDTO));
        MySqlSource<RowJson> mySqlSource = CdcUtil.buildMySqlSource(datasourceProperties, mysqlSourceDTO.getDatabaseList(), mysqlSourceDTO.getTableList(),
                odsProperties.getParallelism().getExecution(), odsProperties.getTargetTimeZone(), odsProperties.getStartupMode());

        SingleOutputStreamOperator<RowJson> wholeStream = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), instanceName + "Source")
                .uid(instanceName + "Source")
                //.rebalance()
                //.filter(new DeletedFilterFunction(odsProperties.getApplication(), propertiesCacheFileName))
                //.uid(String.format("%s filter delete ops", instanceName))
                .process(new SplitTableProcessFunction(odsProperties.getApplication(), cacheFileName, propertiesCacheFileName))
                .uid(String.format("%s monitor ddl && output by tag", instanceName));

        String url = DbCommonUtil.buildUrl(datasourceProperties.getHost(), datasourceProperties.getPort(), datasourceProperties.getOneDatabaseName(), datasourceProperties.getTimeZone());

        Set<String> proccessedTables = new HashSet<>();
        Map<String, KafkaSink<String>> kafkaSinkMap = new HashMap<>();
        try (Connection connection = DbCommonUtil.getConnection(datasourceProperties.getUsername(), datasourceProperties.getPassword(), url)) {
            for (Map.Entry<String, TableDTO> entry : availableTables.entrySet()) {
                TableDTO tableDTO = entry.getValue();
                // 已处理的不再处理，否则多个线程操作输出流会报错
                String targetDbNameAndTableName = tableDTO.getDbName() + CdcConstants.DOT + tableDTO.getTableName();
                if (!proccessedTables.add(targetDbNameAndTableName)) {
                    continue;
                }

                String targetDbName = Namespaces.getOdsPre(odsProperties.getEnv()) + tableDTO.getDbName();
                Namespace namespace = Namespace.of(targetDbName);
                TableIdentifier identifier = TableIdentifier.of(namespace, tableDTO.getTableName());
                Table table = IcebergUtil.getExistTable(hdfsProperties, targetDbName, tableDTO.getTableName());
                if (table == null) {
                    String[] strs = entry.getKey().split(CdcConstants.ESCAPE_DOT);
                    String sourceDbName = strs[0];
                    String sourceTableName = strs[1];
                    List<ColumnMetaDataDTO> columnMetaDatas = null;
                    try {
                        columnMetaDatas = DbCommonUtil.getTableColumnMetaDatas(connection, sourceDbName, sourceTableName);
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
                SideOutputDataStream<RowJson> tableStream = wholeStream.getSideOutput(outputTag);
                DataStream<RowData> icebergStream = null;
                if (StringUtils.isNotBlank(odsProperties.getKafkaBootstrapServers())) {
                    // 增量数据分流
                    SingleOutputStreamOperator<RowJson> specificTableSplitStream = tableStream
                            .process(new SplitMQProcessFunction(tableDTO.getDbName(), tableDTO.getTableName()))
                            .uid(targetDbNameAndTableName + " mq split");
                    // 增量数据发mq
                    String topic = MqUtil.getTopic(odsProperties.getEnv(), tableDTO.getDbName(), tableDTO.getTableName());
                    // 检查topic是否存在，不存在则创建
                    if (!topics.contains(topic)) {
                        NewTopic newTopic = new NewTopic(topic, 1, (short) 1);
                        adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
                    }
                    KafkaSink<String> kafkaSink = kafkaSinkMap.get(topic);
                    if (kafkaSink == null) {
                        kafkaSink = KafkaSink.<String>builder()
                                .setBootstrapServers(odsProperties.getKafkaBootstrapServers())
                                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, String.valueOf((15 * 60 - 5) * 1000))
                                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                                .setTransactionalIdPrefix(String.format("ods_%s", topic))
                                .setRecordSerializer(
                                        KafkaRecordSerializationSchema.builder()
                                                .setTopic(topic)
                                                .setKeySerializationSchema(new SimpleStringSchema())
                                                .setValueSerializationSchema(new SimpleStringSchema())
                                                .build())
                                .build();

                        kafkaSinkMap.put(topic, kafkaSink);
                    }

                    specificTableSplitStream.getSideOutput(OutputTagCache.getMQOutputTag(tableDTO.getDbName(), tableDTO.getTableName()))
                            .sinkTo(kafkaSink)
                            .uid(targetDbNameAndTableName + " sink mq");


                    icebergStream = specificTableSplitStream.flatMap(new RowJsonConvertFunction(FlinkSchemaUtil.convert(schema), tableDTO.isSharding()))
                            .uid(targetDbNameAndTableName + " convert");
                } else {
                    icebergStream = tableStream.flatMap(new RowJsonConvertFunction(FlinkSchemaUtil.convert(schema), tableDTO.isSharding()))
                            .uid(targetDbNameAndTableName + " convert");
                }

                TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, identifier);
                // 写iceberg
                FlinkSink.forRowData(icebergStream)
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
        kafkaSinkMap.clear();
        kafkaSinkMap = null;
    }

    /**
     * 创建namespace
     *
     * @param datasourceProperties
     * @param hiveCatalog
     * @return 返回需要同步的库表信息
     */
    private static MysqlSourceDTO createNamespace(FlinkDatasourceProperties datasourceProperties, HiveCatalog hiveCatalog, String env) {
        Map<String, FlinkDatasourceDetailProperties> details = datasourceProperties.getDetails();
        // MySqlSource入参databaseList
        Set<String> dbNames = new HashSet<>();
        // MySqlSource入参tableList
        Set<String> tableNames = new HashSet<>();
        for (Map.Entry<String, FlinkDatasourceDetailProperties> entry : details.entrySet()) {
            String dbName = entry.getKey();
            FlinkDatasourceDetailProperties sourceDetail = entry.getValue();

            FlinkDatasourceShardingProperties sharding = sourceDetail.getSharding();

            String url = DbCommonUtil.buildUrl(datasourceProperties.getHost(), datasourceProperties.getPort(), dbName, datasourceProperties.getTimeZone());
            try (Connection connection = DbCommonUtil.getConnection(datasourceProperties.getUsername(), datasourceProperties.getPassword(), url)) {
                Set<String> tableList = DbCommonUtil.getTables(connection, sourceDetail.getType(), sourceDetail.getTables(), sourceDetail.getSharding().getTables());
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
            Namespace namespace = Namespace.of(Namespaces.getOdsPre(env) + dbNameTarget);
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