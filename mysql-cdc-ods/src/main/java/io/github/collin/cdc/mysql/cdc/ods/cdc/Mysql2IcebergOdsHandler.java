package io.github.collin.cdc.mysql.cdc.ods.cdc;

import io.github.collin.cdc.common.constants.CdcConstants;
import io.github.collin.cdc.common.enums.Env;
import io.github.collin.cdc.common.enums.Namespaces;
import io.github.collin.cdc.common.properties.HdfsProperties;
import io.github.collin.cdc.mysql.cdc.ods.cache.OutputTagCache;
import io.github.collin.cdc.mysql.cdc.ods.util.IcebergUtil;
import io.github.collin.cdc.common.util.JacksonUtil;
import io.github.collin.cdc.common.util.MqUtil;
import io.github.collin.cdc.mysql.cdc.common.dto.ColumnMetaDataDTO;
import io.github.collin.cdc.mysql.cdc.common.dto.RowJson;
import io.github.collin.cdc.mysql.cdc.common.dto.TableDTO;
import io.github.collin.cdc.mysql.cdc.common.util.DbCommonUtil;
import io.github.collin.cdc.mysql.cdc.ods.exception.PrimaryKeyStateException;
import io.github.collin.cdc.mysql.cdc.ods.function.RowJsonConvertFunction;
import io.github.collin.cdc.mysql.cdc.ods.function.SplitMQProcessFunction;
import io.github.collin.cdc.mysql.cdc.ods.properties.IcebergOdsProperties;
import io.github.collin.cdc.mysql.cdc.ods.util.PartitionUtil;
import io.github.collin.cdc.mysql.cdc.ods.util.SinkIcebergUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.admin.AdminClient;
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.admin.NewTopic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
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
import java.util.function.BiFunction;

/**
 * mysql数据源同步到iceberg（ods层）
 *
 * @author collin
 * @date 2023-03-31
 */
@Slf4j
public class Mysql2IcebergOdsHandler extends AbstractMysqlCdcHandler<IcebergOdsProperties> {

    private CatalogLoader catalogLoader = null;
    private HiveCatalog hiveCatalog = null;

    public Mysql2IcebergOdsHandler(IcebergOdsProperties odsProperties) throws IOException {
        super(odsProperties);

        HdfsProperties hdfsProperties = odsProperties.getHdfs();
        this.catalogLoader = IcebergUtil.catalogConfiguration(hdfsProperties);
        this.hiveCatalog = (HiveCatalog) catalogLoader.loadCatalog();
    }

    @Override
    protected BiFunction<Env, String, Void> buildCreateDatabaseFunction() {
        return (env, dbNameTarget) -> {
            Namespace namespace = Namespace.of(Namespaces.getOdsPre(env) + dbNameTarget);
            if (!hiveCatalog.namespaceExists(namespace)) {
                System.out.printf("---->start create namespace[%s]%n", namespace.toString());
                hiveCatalog.createNamespace(namespace);
                System.out.printf("---->end create namespace[%s]%n", namespace);
            }
            return null;
        };
    }

    @Override
    protected void createTablesAndSink(Map<String, TableDTO> availableTables, Connection connection, SingleOutputStreamOperator<RowJson> wholeStream,
                                       IcebergOdsProperties odsProperties, AdminClient adminClient, Set<String> topics) throws Exception {
        Set<String> proccessedTables = new HashSet<>();
        Map<String, KafkaSink<String>> kafkaSinkMap = new HashMap<>();
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
            Table table = IcebergUtil.getExistTable(odsProperties.getHdfs(), targetDbName, tableDTO.getTableName());
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

                Set<String> partitionFields = PartitionUtil.getPartitionFieldSet(targetDbNameAndTableName);
                List<Types.NestedField> nestedFields = SinkIcebergUtil.getColumns(columnMetaDatas, partitionFields, tableDTO.isSharding());
                Set<Integer> primaryKeyNames = SinkIcebergUtil.getPrimaryKeyNames(columnMetaDatas, partitionFields, tableDTO.isSharding());
                Schema schema = new Schema(nestedFields, primaryKeyNames);

                System.out.printf("---->start create table[%s]%n", identifier);
                table = IcebergUtil.createTable(schema, hiveCatalog, identifier, SinkIcebergUtil.buildPartitionSpec(schema, targetDbNameAndTableName));
                System.out.printf("---->end create table[%s]%n", identifier);
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
                        .sinkTo(kafkaSink)
                        .uid(targetDbNameAndTableName + " sink mq");


                icebergStream = specificTableSplitStream.flatMap(new RowJsonConvertFunction(FlinkSchemaUtil.convert(schema), tableDTO.isSharding()))
                        .uid(targetDbNameAndTableName + " convert")
                        .name(targetDbNameAndTableName + " convert")
                        .rebalance();
            } else {
                icebergStream = tableStream.flatMap(new RowJsonConvertFunction(FlinkSchemaUtil.convert(schema), tableDTO.isSharding()))
                        .uid(targetDbNameAndTableName + " convert")
                        .name(targetDbNameAndTableName + " convert")
                        .rebalance();
            }

            TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, identifier);

            System.out.println(targetDbNameAndTableName + "-->" + JacksonUtil.toJson(identifierFieldNames));

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
    }

}