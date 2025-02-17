package io.github.collin.cdc.mysql.cdc.iceberg.cdc;

import com.starrocks.connector.flink.StarRocksSink;
import com.starrocks.connector.flink.catalog.StarRocksColumn;
import com.starrocks.connector.flink.catalog.StarRocksTable;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import io.github.collin.cdc.common.constants.CdcConstants;
import io.github.collin.cdc.common.enums.Env;
import io.github.collin.cdc.common.enums.Namespaces;
import io.github.collin.cdc.common.util.JacksonUtil;
import io.github.collin.cdc.common.util.MqUtil;
import io.github.collin.cdc.mysql.cdc.common.dto.ColumnMetaDataDTO;
import io.github.collin.cdc.mysql.cdc.common.dto.RowJson;
import io.github.collin.cdc.mysql.cdc.common.dto.TableDTO;
import io.github.collin.cdc.mysql.cdc.common.util.DbCommonUtil;
import io.github.collin.cdc.mysql.cdc.iceberg.cache.OutputTagCache;
import io.github.collin.cdc.mysql.cdc.iceberg.dto.PartitionFieldDTO;
import io.github.collin.cdc.mysql.cdc.iceberg.exception.PrimaryKeyStateException;
import io.github.collin.cdc.mysql.cdc.iceberg.function.SplitMQProcessFunction;
import io.github.collin.cdc.mysql.cdc.iceberg.properties.StarRocksOdsProperties;
import io.github.collin.cdc.mysql.cdc.iceberg.properties.StarRocksProperties;
import io.github.collin.cdc.mysql.cdc.iceberg.starrocks.EnhanceStarRocksCatalog;
import io.github.collin.cdc.mysql.cdc.iceberg.starrocks.JsonRowProcess;
import io.github.collin.cdc.mysql.cdc.iceberg.starrocks.JsonRowSerializer;
import io.github.collin.cdc.mysql.cdc.iceberg.util.SinkStarRocksUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.admin.AdminClient;
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.admin.NewTopic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.OutputTag;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.function.BiFunction;

/**
 * mysql数据源同步到StarRocks（ods层）
 *
 * @author collin
 * @date 2024-11-28
 */
@Slf4j
public class Mysql2StarRocksOdsHandler extends AbstractMysqlCdcHandler<StarRocksOdsProperties> {

    private EnhanceStarRocksCatalog starRocksCatalog = null;
    private StarRocksProperties starRocksProperties = null;

    public Mysql2StarRocksOdsHandler(StarRocksOdsProperties odsProperties) throws IOException {
        super(odsProperties);

        starRocksProperties = odsProperties.getStarRocks();
        starRocksCatalog = new EnhanceStarRocksCatalog(starRocksProperties.getJdbcUrl(), starRocksProperties.getUsername(), starRocksProperties.getPassword());
    }

    @Override
    protected BiFunction<Env, String, Void> buildCreateDatabaseFunction() {
        return (env, dbNameTarget) -> {
            try {
                starRocksCatalog.open();
                String database = Namespaces.getOdsPre(env) + dbNameTarget;
                if (!starRocksCatalog.databaseExists(database)) {
                    System.out.printf("---->start create namespace[%s]%n", database);
                    starRocksCatalog.createDatabase(database, true);
                    System.out.printf("---->end create namespace[%s]%n", database);
                }
            } finally {
                starRocksCatalog.close();
            }
            return null;
        };
    }

    private void cleanTables(Boolean openCleanTable, Map<String, TableDTO> availableTables) {
        if (!openCleanTable) {
            return;
        }
        Set<String> proccessedTables = new HashSet<>();
        for (TableDTO tableDTO : availableTables.values()) {
            String targetDbName = Namespaces.getOdsPre(odsProperties.getEnv()) + tableDTO.getDbName();
            if (proccessedTables.add(targetDbName + CdcConstants.DOT + tableDTO.getTableName())) {
                if (starRocksCatalog.tableExists(targetDbName, tableDTO.getTableName())) {
                    System.out.printf("---->start drop table[%s.%s]%n", targetDbName, tableDTO.getTableName());
                    starRocksCatalog.dropTable(targetDbName, tableDTO.getTableName());
                    System.out.printf("---->end drop table[%s.%s]%n", targetDbName, tableDTO.getTableName());
                }
            }
        }
    }

    @Override
    protected void createTablesAndSink(Map<String, TableDTO> availableTables, Connection connection, SingleOutputStreamOperator<RowJson> wholeStream,
                                       StarRocksOdsProperties odsProperties, AdminClient adminClient, Set<String> topics) throws Exception {
        Set<String> proccessedTables = new HashSet<>();
        Map<String, KafkaSink<String>> kafkaSinkMap = new HashMap<>();
        try {
            starRocksCatalog.open();
            cleanTables(odsProperties.getOpenCleanTable(), availableTables);

            for (Map.Entry<String, TableDTO> entry : availableTables.entrySet()) {
                TableDTO tableDTO = entry.getValue();
                // 已处理的不再处理，否则多个线程操作输出流会报错
                String targetDbNameAndTableName = tableDTO.getDbName() + CdcConstants.DOT + tableDTO.getTableName();
                if (!proccessedTables.add(targetDbNameAndTableName)) {
                    continue;
                }

                String targetDbName = Namespaces.getOdsPre(odsProperties.getEnv()) + tableDTO.getDbName();
                if (!starRocksCatalog.tableExists(targetDbName, tableDTO.getTableName())) {
                    String[] strs = entry.getKey().split(CdcConstants.ESCAPE_DOT);
                    String sourceDbName = strs[0];
                    String sourceTableName = strs[1];
                    List<ColumnMetaDataDTO> columnMetaDatas = null;
                    try {
                        columnMetaDatas = DbCommonUtil.getTableColumnMetaDatas(connection, sourceDbName, sourceTableName);
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }

                    PartitionFieldDTO partitionFieldDto = SinkStarRocksUtil.getPartitionField(targetDbNameAndTableName);
                    String partitionField = partitionFieldDto == null ? null : partitionFieldDto.getName();
                    List<String> primaryKeyNames = SinkStarRocksUtil.getPrimaryKeyNames(columnMetaDatas, partitionField, tableDTO.isSharding());
                    if (primaryKeyNames == null || primaryKeyNames.isEmpty()) {
                        throw new PrimaryKeyStateException(String.format("[%s.%s] primaryKeyNames is null", tableDTO.getDbName(), tableDTO.getTableName()));
                    }
                    System.out.println(targetDbNameAndTableName + "-->" + JacksonUtil.toJson(primaryKeyNames));

                    List<StarRocksColumn> columns = SinkStarRocksUtil.buildStarRocksColumns(columnMetaDatas, primaryKeyNames, tableDTO.isSharding());
                    Integer numBuckets = SinkStarRocksUtil.getNumBuckets(targetDbNameAndTableName);
                    // 默认设置桶为1
                    if (numBuckets == null) {
                        numBuckets = 1;
                    }
                    Map<String, String> properties = new HashMap<>(2);
                    properties.put("enable_persistent_index", "true");
                    properties.put("in_memory", "false");

                    StarRocksTable.Builder starRocksSchemaBuilder = new StarRocksTable.Builder()
                            .setDatabaseName(targetDbName)
                            .setTableName(tableDTO.getTableName())
                            .setComment(DbCommonUtil.getTablesComment(connection, sourceDbName, sourceTableName))
                            .setColumns(columns)
                            .setNumBuckets(numBuckets)
                            .setTableType(StarRocksTable.TableType.PRIMARY_KEY)
                            .setDistributionKeys(primaryKeyNames)
                            .setTableKeys(primaryKeyNames)
                            .setTableProperties(properties);
                    String partitionSql = null;
                    if (partitionFieldDto != null) {
                        partitionSql = SinkStarRocksUtil.buildPartitionSql(partitionFieldDto);
                    }
                    starRocksCatalog.createTable(starRocksSchemaBuilder.build(), partitionSql, true);
                }

                OutputTag<RowJson> outputTag = OutputTagCache.getOutputTag(tableDTO.getDbName(), tableDTO.getTableName());
                DataStream<RowJson> tableStream = wholeStream.getSideOutput(outputTag).rebalance();
                DataStream<String> starRocksStream = null;
                JsonRowSerializer jsonRowSerializer = SinkStarRocksUtil.buildJsonRowSerializer(targetDbName, tableDTO.getTableName(), starRocksProperties.getJdbcUrl(),
                        starRocksProperties.getUsername(), starRocksProperties.getPassword());

                if (StringUtils.isNotBlank(odsProperties.getKafkaBootstrapServers())) {
                    // 增量数据分流
                    SingleOutputStreamOperator<RowJson> specificTableSplitStream = tableStream
                            .process(new SplitMQProcessFunction(tableDTO.getDbName(), tableDTO.getTableName()))
                            .uid(targetDbNameAndTableName + " mq split")
                            .name(targetDbNameAndTableName + " mq split");
                    // 增量数据发mq
                    String topic = MqUtil.getTopic(odsProperties.getEnv(), tableDTO.getDbName(), tableDTO.getTableName());
                    // 检查topic是否存在，不存在则创建
                    if (!topics.contains(topic)) {
                        NewTopic newTopic = new NewTopic(topic, 1, (short) 1);
                        adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
                    }
                    KafkaSink<String> kafkaSink = kafkaSinkMap.get(topic);
                    if (kafkaSink == null) {
                        kafkaSink = buildKafkaSink(topic);

                        kafkaSinkMap.put(topic, kafkaSink);
                    }

                    specificTableSplitStream.getSideOutput(OutputTagCache.getMQOutputTag(tableDTO.getDbName(), tableDTO.getTableName()))
                            .rebalance()
                            .sinkTo(kafkaSink)
                            .uid(targetDbNameAndTableName + " sink mq")
                            .name(targetDbNameAndTableName + " sink mq");


                    starRocksStream = specificTableSplitStream.process(new JsonRowProcess(jsonRowSerializer, tableDTO.isSharding()))
                            .uid(targetDbNameAndTableName + " convert")
                            .name(targetDbNameAndTableName + " convert");
                } else {
                    starRocksStream = tableStream.process(new JsonRowProcess(jsonRowSerializer, tableDTO.isSharding()))
                            .uid(targetDbNameAndTableName + " process")
                            .name(targetDbNameAndTableName + " process");
                }

                StarRocksSinkOptions starRocksSinkOptions = SinkStarRocksUtil.buildStarRocksSinkOptions(starRocksProperties, targetDbName, tableDTO.getTableName());
                starRocksStream
                        .rebalance()
                        .addSink(StarRocksSink.sink(starRocksSinkOptions))
                        .setParallelism(odsProperties.getParallelism().getWrite())
                        .name(targetDbNameAndTableName);
            }
        } finally {
            starRocksCatalog.close();
        }
    }

}