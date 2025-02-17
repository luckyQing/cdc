package io.github.collin.cdc.mysql.cdc.ods.cdc;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import io.github.collin.cdc.common.constants.CdcConstants;
import io.github.collin.cdc.common.enums.Env;
import io.github.collin.cdc.common.util.FlinkUtil;
import io.github.collin.cdc.common.util.JacksonUtil;
import io.github.collin.cdc.common.util.RedisKeyUtil;
import io.github.collin.cdc.mysql.cdc.common.dto.RowJson;
import io.github.collin.cdc.mysql.cdc.common.dto.TableDTO;
import io.github.collin.cdc.mysql.cdc.common.listener.FlinkJobListener;
import io.github.collin.cdc.mysql.cdc.common.properties.FlinkDatasourceDetailProperties;
import io.github.collin.cdc.mysql.cdc.common.properties.FlinkDatasourceProperties;
import io.github.collin.cdc.mysql.cdc.common.properties.FlinkDatasourceShardingProperties;
import io.github.collin.cdc.mysql.cdc.common.util.DbCommonUtil;
import io.github.collin.cdc.mysql.cdc.ods.dto.MysqlSourceDTO;
import io.github.collin.cdc.mysql.cdc.ods.dto.cache.PropertiesCacheDTO;
import io.github.collin.cdc.mysql.cdc.ods.enums.SourceType;
import io.github.collin.cdc.mysql.cdc.ods.function.DeletedFilterFunction;
import io.github.collin.cdc.mysql.cdc.ods.function.SplitTableProcessFunction;
import io.github.collin.cdc.mysql.cdc.ods.properties.AbstractOdsProperties;
import io.github.collin.cdc.mysql.cdc.ods.source.FixDataMysqlSource;
import io.github.collin.cdc.mysql.cdc.ods.util.CdcUtil;
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
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.redisson.api.RedissonClient;

import java.io.IOException;
import java.sql.Connection;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import io.github.collin.cdc.common.common.adapter.RedisAdapter;

@Slf4j
public abstract class AbstractMysqlCdcHandler<T extends AbstractOdsProperties> {

    protected final T odsProperties;

    public AbstractMysqlCdcHandler(T abstractOdsProperties) throws IOException {
        this.odsProperties = abstractOdsProperties;
    }

    /**
     * 创建数据库
     *
     * @return
     */
    protected abstract BiFunction<Env, String, Void> buildCreateDatabaseFunction();

    /**
     * @param availableTables
     * @param connection
     * @param wholeStream
     * @param abstractOdsProperties
     * @param adminClient
     * @param topics
     * @throws Exception
     */
    protected abstract void createTablesAndSink(Map<String, TableDTO> availableTables, Connection connection, SingleOutputStreamOperator<RowJson> wholeStream,
                                                T abstractOdsProperties, AdminClient adminClient, Set<String> topics) throws Exception;

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

        AdminClient adminClient = null;
        Set<String> topics = null;
        RedissonClient redissonClient = null;

        try {
            if (StringUtils.isNotBlank(odsProperties.getKafkaBootstrapServers())) {
                Properties adminProperties = new Properties();
                adminProperties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, odsProperties.getKafkaBootstrapServers());
                adminClient = AdminClient.create(adminProperties);
                topics = adminClient.listTopics().names().get();
            }

            redissonClient = new RedisAdapter(odsProperties.getRedis()).getRedissonClient();
            // 将properties缓存到redis中供后面使用
            PropertiesCacheDTO propertiesCacheDTO = new PropertiesCacheDTO();
            propertiesCacheDTO.setProxy(odsProperties.getProxy());
            propertiesCacheDTO.setMonitor(odsProperties.getMonitor());
            redissonClient.getBucket(RedisKeyUtil.buildPropertiesKey(odsProperties.getApplication()))
                    .set(JacksonUtil.toJson(propertiesCacheDTO));

            for (Map.Entry<String, FlinkDatasourceProperties> entry : datasources.entrySet()) {
                proccessInstance(entry.getKey(), env, odsProperties, entry.getValue(), redissonClient, adminClient, topics);
            }

            env.getJobListeners().add(new FlinkJobListener(env, odsProperties.getApplication(), odsProperties.getRedis()));
        } finally {
            // 释放
            if (topics != null) {
                topics.clear();
                topics = null;
            }
            if (adminClient != null) {
                adminClient.close();
            }
            if (redissonClient != null) {
                redissonClient.shutdown();
            }
        }

        env.execute(String.format("mysql-%s sync database: %s(ods)", odsProperties.getSinkType().toLowerCase(), odsProperties.getApplication()));
    }

    /**
     * 处理单个数据库实例
     *
     * @param instanceName
     * @param streamExecutionEnvironment
     * @param abstractOdsProperties
     * @param datasourceProperties
     * @param redissonClient
     * @throws IOException
     */
    protected void proccessInstance(String instanceName, StreamExecutionEnvironment streamExecutionEnvironment, T abstractOdsProperties, FlinkDatasourceProperties datasourceProperties,
                                    RedissonClient redissonClient, AdminClient adminClient, Set<String> topics) throws IOException {
        Map<String, FlinkDatasourceDetailProperties> details = datasourceProperties.getDetails();
        // 获取所有可用的数据库，并缓存关系<源数据库名, 目标数据库名>
        Map<String, String> availableDatabases = DbCommonUtil.listAvailableDatabases(datasourceProperties, details);

        // 获取所有可用的表，并缓存关系<源数据库名.源表名, 目标表名>
        Map<String, TableDTO> availableTables = DbCommonUtil.listAvailableTables(datasourceProperties, details);

        // 映射关系
        Map<String, String> relations = buildRelation(availableDatabases, availableTables);

        // 将映射关系缓存redis中供后面使用
        redissonClient.getBucket(RedisKeyUtil.buildRelationsKey(abstractOdsProperties.getApplication(), instanceName))
                .set(JacksonUtil.toJson(relations), 3650, TimeUnit.DAYS);

        Set<String> allExcludeDeleteTables = new HashSet<>();
        for (Map.Entry<String, FlinkDatasourceDetailProperties> entry : details.entrySet()) {
            Set<String> excludeDeleteTables = entry.getValue().getExcludeDeleteTables();
            if (excludeDeleteTables != null && !excludeDeleteTables.isEmpty()) {
                allExcludeDeleteTables.addAll(excludeDeleteTables.stream().map(x -> entry.getKey() + "." + x).collect(Collectors.toSet()));
            }
        }

        // 物理删除表过滤
        redissonClient.getBucket(RedisKeyUtil.buildExcludeDeleteTableKey(abstractOdsProperties.getApplication(), instanceName))
                .set(JacksonUtil.toJson(allExcludeDeleteTables), 3650, TimeUnit.DAYS);

        MysqlSourceDTO mysqlSourceDTO = createDatabase(datasourceProperties, Env.valueOf(abstractOdsProperties.getEnv()));

        System.out.println("source:" + JacksonUtil.toJson(mysqlSourceDTO));

        DataStreamSource<RowJson> sourceStream = null;
        if (SourceType.FIX_DATA_MYSQL_SOURCE.name().equals(odsProperties.getSourceType())) {
            List<String> sourceTableNames = Arrays.stream(mysqlSourceDTO.getTableList()).map(item -> item.split("\\.")[1]).collect(Collectors.toList());
            RichSourceFunction<RowJson> source = new FixDataMysqlSource(datasourceProperties, mysqlSourceDTO.getDatabaseList()[0], sourceTableNames, odsProperties.getFixSql());
            sourceStream = streamExecutionEnvironment.addSource(source);
        } else {
            MySqlSource<RowJson> source = CdcUtil.buildMySqlSource(datasourceProperties, mysqlSourceDTO.getDatabaseList(), mysqlSourceDTO.getTableList(),
                    abstractOdsProperties.getParallelism().getExecution(), abstractOdsProperties.getTargetTimeZone(), abstractOdsProperties.getStartupMode());
            sourceStream = streamExecutionEnvironment.fromSource(source, WatermarkStrategy.noWatermarks(), instanceName + "Source");
        }

        SingleOutputStreamOperator<RowJson> wholeStream = sourceStream
                .uid(instanceName + "Source")
                .rebalance()
                .filter(new DeletedFilterFunction(abstractOdsProperties.getRedis(), abstractOdsProperties.getApplication(), instanceName))
                .uid(String.format("%s filter delete ops", instanceName))
                .name(String.format("%s filter delete ops", instanceName))
                .rebalance()
                .process(new SplitTableProcessFunction(abstractOdsProperties.getRedis(), abstractOdsProperties.getApplication(), instanceName, abstractOdsProperties.getSinkType()))
                .uid(String.format("%s monitor ddl && output by tag", instanceName))
                .name(String.format("%s monitor ddl && output by tag", instanceName));

        String url = DbCommonUtil.buildUrl(datasourceProperties.getHost(), datasourceProperties.getPort(), datasourceProperties.getOneDatabaseName(), datasourceProperties.getTimeZone());

        try (Connection connection = DbCommonUtil.getConnection(datasourceProperties.getUsername(), datasourceProperties.getPassword(), url)) {
            createTablesAndSink(availableTables, connection, wholeStream, abstractOdsProperties, adminClient, topics);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
    protected Map<String, String> buildRelation(Map<String, String> availableDatabases, Map<String, TableDTO> availableTables) {
        Map<String, String> relations = new HashMap<>(availableDatabases.size() + availableTables.size());
        relations.putAll(availableDatabases);
        for (Map.Entry<String, TableDTO> entry : availableTables.entrySet()) {
            relations.put(entry.getKey(), entry.getValue().getTableName());
        }

        return relations;
    }

    /**
     * 创建kafka sink
     *
     * @param topic
     * @return
     */
    protected KafkaSink<String> buildKafkaSink(String topic) {
        return KafkaSink.<String>builder()
                .setBootstrapServers(odsProperties.getKafkaBootstrapServers())
                .setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, String.valueOf(1024 * 1024 * 50))
                .setProperty("buffer.memory", String.valueOf(1024 * 1024 * 50))
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
    }

    /**
     * 创建数据库（namespace）
     *
     * @param datasourceProperties
     * @param env
     * @return 返回需要同步的库表信息
     */
    protected MysqlSourceDTO createDatabase(FlinkDatasourceProperties datasourceProperties, Env env) {
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
            buildCreateDatabaseFunction().apply(env, dbNameTarget);

            String dbNameSource = StringUtils.isNotBlank(sharding.getDbNameSource()) ? sharding.getDbNameSource() : dbName;
            dbNames.add(dbNameSource);
        }

        return new MysqlSourceDTO(dbNames.toArray(new String[0]), tableNames.toArray(new String[0]));
    }

}