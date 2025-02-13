package io.github.collin.cdc.mysql.cdc.mysql.function;

import com.mysql.cj.jdbc.Driver;
import com.ververica.cdc.connectors.shaded.com.google.common.collect.HashBasedTable;
import io.github.collin.cdc.common.common.adapter.RedisAdapter;
import io.github.collin.cdc.common.constants.CdcConstants;
import io.github.collin.cdc.common.constants.SqlConstants;
import io.github.collin.cdc.common.enums.OpType;
import io.github.collin.cdc.common.properties.ProxyProperties;
import io.github.collin.cdc.common.properties.RedisProperties;
import io.github.collin.cdc.common.properties.RobotProperties;
import io.github.collin.cdc.common.util.JacksonUtil;
import io.github.collin.cdc.mysql.cdc.common.constants.DbConstants;
import io.github.collin.cdc.mysql.cdc.common.dto.ColumnMetaDataDTO;
import io.github.collin.cdc.mysql.cdc.common.dto.RowJson;
import io.github.collin.cdc.mysql.cdc.mysql.adapter.RobotAdapter;
import io.github.collin.cdc.mysql.cdc.mysql.constants.FieldConstants;
import io.github.collin.cdc.mysql.cdc.mysql.constants.JdbcConstants;
import io.github.collin.cdc.mysql.cdc.mysql.dto.JdbcOutputFormatDTO;
import io.github.collin.cdc.mysql.cdc.mysql.dto.TableDTO;
import io.github.collin.cdc.mysql.cdc.mysql.dto.cache.ConfigCacheDTO;
import io.github.collin.cdc.mysql.cdc.mysql.enums.TableShardingType;
import io.github.collin.cdc.mysql.cdc.mysql.properties.DatasourceProperties;
import io.github.collin.cdc.mysql.cdc.mysql.util.DbUtil;
import io.github.collin.cdc.mysql.cdc.mysql.util.JdbcUtil;
import io.github.collin.cdc.mysql.cdc.mysql.util.MigrationRedisKeyUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.internal.JdbcOutputFormat;
import org.apache.flink.connector.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Preconditions;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

/**
 * sink实现
 *
 * @author collin
 * @date 2023-07-20
 * @see org.apache.flink.connector.jdbc.internal.GenericJdbcSinkFunction
 */
@Slf4j
public class GenericJdbcSinkAdapterFunction extends RichSinkFunction<RowJson> {

    /**
     * 实例名
     */
    private final String instanceName;
    private final RedisProperties redisProperties;
    /**
     * 代理
     */
    private final ProxyProperties proxyProperties;
    /**
     * ddl接收监控
     */
    private final RobotProperties robotProperties;

    private transient RobotAdapter robotAdapter = null;
    /**
     * 库：<源数据库名, 数据库index>
     */
    private transient Map<String, String> dbIndexs = null;
    /**
     * 表：<源数据库名, 源表名, 表index>
     */
    private transient HashBasedTable<String, String, String> tableIndexs = null;
    /**
     * 执行sql<源库名，源表名，JdbcOutputFormatDTO>
     */
    private transient HashBasedTable<String, String, JdbcOutputFormatDTO> sourceOutputFormats = null;

    public GenericJdbcSinkAdapterFunction(@Nonnull String instanceName, RedisProperties redisProperties, ProxyProperties proxyProperties, RobotProperties robotProperties) {
        this.instanceName = Preconditions.checkNotNull(instanceName);
        this.redisProperties = redisProperties;
        this.proxyProperties = proxyProperties;
        this.robotProperties = robotProperties;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.robotAdapter = new RobotAdapter(proxyProperties, robotProperties);

        ConfigCacheDTO configCacheDTO = getConfigCacheDTO();
        Map<String, TableDTO> tableRelations = configCacheDTO.getTableRelations();
        DatasourceProperties targetDatasource = configCacheDTO.getTargetDatasource();

        try (Connection connection = DbUtil.getConnection(targetDatasource.getUsername(), targetDatasource.getPassword(),
                targetDatasource.getHost(), targetDatasource.getPort(), DbConstants.INFORMATION_SCHEMA_DBNAME,
                targetDatasource.getTimeZone())) {
            // 初始化JdbcOutputFormat
            initSourceOutputFormats(connection, tableRelations, targetDatasource, targetDatasource.getTimeZone());
        }

        initShardingIndex(tableRelations);
    }

    @Override
    public void invoke(RowJson value, Context context) throws IOException {
        if (value.getOp() == OpType.DDL) {
            // 企业微信通知
            robotAdapter.noticeAfterReceiveDdl(value.getDb(), value.getTable(), value.getDdl());
            return;
        }

        String sourceDb = value.getDb();
        String sourceTable = value.getTable();
        JdbcOutputFormatDTO jdbcOutputFormatDTO = sourceOutputFormats.get(sourceDb, sourceTable);

        Map<String, Object> fieldValueMappings = value.getJson();

        int tableShardingIndex = 0;
        // 是否有合并字段
        if (jdbcOutputFormatDTO.isMergeFields()) {
            fieldValueMappings.put(SqlConstants.FIELD_DB_INDEX, dbIndexs.get(sourceDb));
            fieldValueMappings.put(SqlConstants.FIELD_TABLE_INDEX, tableIndexs.get(sourceDb, sourceTable));
        }
        // 需要合并的表写入分片数据
        TableShardingType shardingType = jdbcOutputFormatDTO.getShardingType();
        if (shardingType != null && shardingType != TableShardingType.ONE) {
            String shardingFieldName = jdbcOutputFormatDTO.getShardingFieldName();
            Object shardingFieldValue = fieldValueMappings.get(jdbcOutputFormatDTO.getShardingFieldName());
            if (FieldConstants.SHARDING_COLUMN_NAME_UID.equals(shardingFieldName)) {
                int uid = (int) shardingFieldValue;
                tableShardingIndex = uid % shardingType.getTableCount();
            } else if (FieldConstants.SHARDING_COLUMN_NAME_ORDER_ID.equals(shardingFieldName)) {
                String orderId = (String) shardingFieldValue;
                String uidTailStr = orderId.substring(orderId.length() - 3);

                int uidTail = Integer.valueOf(uidTailStr);
                tableShardingIndex = uidTail % shardingType.getTableCount();
            } else {
                throw new IllegalArgumentException(String.format("table[%s.%s] sharding field name is illegal-->%s", sourceDb, sourceTable, JacksonUtil.toJson(fieldValueMappings)));
            }
        }

        if (value.getOp() == OpType.INSERT) {
            jdbcOutputFormatDTO.getInsertOurputFormats().get(tableShardingIndex).writeRecord(fieldValueMappings);
        } else if (value.getOp() == OpType.UPDATE_AFTER) {
            jdbcOutputFormatDTO.getUpdateOurputFormats().get(tableShardingIndex).writeRecord(fieldValueMappings);
        } else if (value.getOp() == OpType.DELETE) {
            jdbcOutputFormatDTO.getDeleteOurputFormats().get(tableShardingIndex).writeRecord(fieldValueMappings);
        }
    }

    @Override
    public void close() throws IOException {
        Collection<JdbcOutputFormatDTO> jdbcOutputFormatDtos = sourceOutputFormats.values();
        for (JdbcOutputFormatDTO jdbcOutputFormatDTO : jdbcOutputFormatDtos) {
            List<JdbcOutputFormat<Map<String, Object>, ?, ?>> insertOurputFormats = jdbcOutputFormatDTO.getInsertOurputFormats();
            for (JdbcOutputFormat<Map<String, Object>, ?, ?> insertOurputFormat : insertOurputFormats) {
                insertOurputFormat.flush();
            }

            List<JdbcOutputFormat<Map<String, Object>, ?, ?>> updateOurputFormats = jdbcOutputFormatDTO.getUpdateOurputFormats();
            for (JdbcOutputFormat<Map<String, Object>, ?, ?> updateOurputFormat : updateOurputFormats) {
                updateOurputFormat.flush();
            }

            List<JdbcOutputFormat<Map<String, Object>, ?, ?>> deleteOurputFormats = jdbcOutputFormatDTO.getDeleteOurputFormats();
            for (JdbcOutputFormat<Map<String, Object>, ?, ?> deleteOurputFormat : deleteOurputFormats) {
                deleteOurputFormat.flush();
            }
        }

        try {
            for (JdbcOutputFormatDTO jdbcOutputFormatDTO : jdbcOutputFormatDtos) {
                List<JdbcOutputFormat<Map<String, Object>, ?, ?>> insertOurputFormats = jdbcOutputFormatDTO.getInsertOurputFormats();
                for (JdbcOutputFormat<Map<String, Object>, ?, ?> insertOurputFormat : insertOurputFormats) {
                    insertOurputFormat.close();
                }

                List<JdbcOutputFormat<Map<String, Object>, ?, ?>> updateOurputFormats = jdbcOutputFormatDTO.getUpdateOurputFormats();
                for (JdbcOutputFormat<Map<String, Object>, ?, ?> updateOurputFormat : updateOurputFormats) {
                    updateOurputFormat.close();
                }

                List<JdbcOutputFormat<Map<String, Object>, ?, ?>> deleteOurputFormats = jdbcOutputFormatDTO.getDeleteOurputFormats();
                for (JdbcOutputFormat<Map<String, Object>, ?, ?> deleteOurputFormat : deleteOurputFormats) {
                    deleteOurputFormat.close();
                }
            }
        } catch (Exception e) {
            log.error("connection close error", e);
        }
    }

    /**
     * 从redis中获取配置
     *
     * @return
     */
    private ConfigCacheDTO getConfigCacheDTO() {
        RedissonClient redissonClient = null;
        try {
            redissonClient = new RedisAdapter(redisProperties).getRedissonClient();
            RMap<String, String> configCache = redissonClient.getMap(MigrationRedisKeyUtil.buildConfigKey());
            String configCacheStr = configCache.get(MigrationRedisKeyUtil.buildConfigHashKey(instanceName));
            return JacksonUtil.parseObject(configCacheStr, ConfigCacheDTO.class);
        } finally {
            if (redissonClient != null) {
                redissonClient.shutdown();
                redissonClient = null;
            }
        }
    }

    /**
     * 初始化JdbcOutputFormat
     *
     * @param connection
     * @param tableRelations
     * @param targetDatasource
     * @param timeZone
     * @throws IOException
     * @throws SQLException
     */
    private void initSourceOutputFormats(Connection connection, Map<String, TableDTO> tableRelations, DatasourceProperties targetDatasource, String timeZone) throws IOException, SQLException {
        // 初始化JdbcOutputFormat
        this.sourceOutputFormats = HashBasedTable.create();
        RuntimeContext ctx = getRuntimeContext();
        JdbcConnectionOptions connectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withDriverName(Driver.class.getName())
                .withUrl(DbUtil.buildUrl(targetDatasource.getHost(), targetDatasource.getPort(), DbConstants.INFORMATION_SCHEMA_DBNAME, timeZone))
                .withUsername(targetDatasource.getUsername())
                .withPassword(targetDatasource.getPassword())
                .build();
        SimpleJdbcConnectionProvider connectionProvider = new SimpleJdbcConnectionProvider(connectionOptions);

        // <目标库，目标表，outputFormat>
        HashBasedTable<String, String, JdbcOutputFormatDTO> targetOutputFormats = HashBasedTable.create();
        for (Map.Entry<String, TableDTO> relation : tableRelations.entrySet()) {
            String[] sourceDbInfo = relation.getKey().split(CdcConstants.ESCAPE_DOT);
            String sourceDbName = sourceDbInfo[0];
            String sourceDbTable = sourceDbInfo[1];
            TableDTO targetDto = relation.getValue();
            String targetDbName = targetDto.getDbName();
            String targetDbTable = targetDto.getTableName();
            JdbcOutputFormatDTO jdbcOutputFormatDTO = targetOutputFormats.get(targetDbName, targetDbTable);
            if (jdbcOutputFormatDTO == null) {
                JdbcExecutionOptions jdbcExecutionOptions = JdbcExecutionOptions.builder()
                        .withBatchSize(targetDto.getBatchSize() <= 0 ? JdbcConstants.BATCH_SIZE : targetDto.getBatchSize())
                        .withBatchIntervalMs(JdbcConstants.BATCH_INTERVAL_MS)
                        .build();
                TableShardingType shardingType = targetDto.getShardingType();
                boolean isMutilTable = shardingType != null && shardingType != TableShardingType.ONE;
                String firstTableName = isMutilTable ? targetDbTable + "_0" : targetDbTable;
                List<ColumnMetaDataDTO> columnMetaDatas = DbUtil.getTableColumnMetaDatas(connection, targetDbName, firstTableName);

                List<JdbcOutputFormat<Map<String, Object>, ?, ?>> insertOurputFormats;
                List<JdbcOutputFormat<Map<String, Object>, ?, ?>> updateOurputFormats;
                List<JdbcOutputFormat<Map<String, Object>, ?, ?>> deleteOurputFormats;
                if (isMutilTable) {
                    insertOurputFormats = new ArrayList<>(shardingType.getTableCount());
                    updateOurputFormats = new ArrayList<>(shardingType.getTableCount());
                    deleteOurputFormats = new ArrayList<>(shardingType.getTableCount());
                    for (int i = 0; i < shardingType.getTableCount(); i++) {
                        String targetTableName = targetDbTable + "_" + i;
                        insertOurputFormats.add(JdbcUtil.buildInsertOurputFormats(ctx, columnMetaDatas, jdbcExecutionOptions, connectionProvider, targetDbName, targetTableName));
                        updateOurputFormats.add(JdbcUtil.buildUpdateOurputFormats(ctx, columnMetaDatas, jdbcExecutionOptions, connectionProvider, targetDbName, targetTableName));
                        deleteOurputFormats.add(JdbcUtil.buildDeleteOurputFormats(ctx, columnMetaDatas, jdbcExecutionOptions, connectionProvider, targetDbName, targetTableName));
                    }
                } else {
                    insertOurputFormats = new ArrayList<>(1);
                    updateOurputFormats = new ArrayList<>(1);
                    deleteOurputFormats = new ArrayList<>(1);

                    insertOurputFormats.add(JdbcUtil.buildInsertOurputFormats(ctx, columnMetaDatas, jdbcExecutionOptions, connectionProvider, targetDbName, targetDbTable));
                    updateOurputFormats.add(JdbcUtil.buildUpdateOurputFormats(ctx, columnMetaDatas, jdbcExecutionOptions, connectionProvider, targetDbName, targetDbTable));
                    deleteOurputFormats.add(JdbcUtil.buildDeleteOurputFormats(ctx, columnMetaDatas, jdbcExecutionOptions, connectionProvider, targetDbName, targetDbTable));
                }

                jdbcOutputFormatDTO = new JdbcOutputFormatDTO();
                jdbcOutputFormatDTO.setInsertOurputFormats(insertOurputFormats);
                jdbcOutputFormatDTO.setUpdateOurputFormats(updateOurputFormats);
                jdbcOutputFormatDTO.setDeleteOurputFormats(deleteOurputFormats);
                jdbcOutputFormatDTO.setShardingType(shardingType);
                jdbcOutputFormatDTO.setShardingFieldName(targetDto.getShardingFieldName());

                if (targetDto.isSharding()) {
                    long shardingFieldCount = columnMetaDatas.stream().filter(x -> SqlConstants.FIELD_DB_INDEX.equals(x.getName())
                            || SqlConstants.FIELD_TABLE_INDEX.equals(x.getName())).count();
                    // 是否需要合并
                    jdbcOutputFormatDTO.setMergeFields(shardingFieldCount == 2);
                }

                targetOutputFormats.put(targetDbName, targetDbTable, jdbcOutputFormatDTO);
            }

            sourceOutputFormats.put(sourceDbName, sourceDbTable, jdbcOutputFormatDTO);
        }

        targetOutputFormats.clear();
        targetOutputFormats = null;
    }

    private void initShardingIndex(Map<String, TableDTO> tableRelations) {
        this.dbIndexs = new HashMap<>();
        this.tableIndexs = HashBasedTable.create();
        for (Map.Entry<String, TableDTO> relation : tableRelations.entrySet()) {
            if (!relation.getValue().isSharding()) {
                continue;
            }

            String[] sourceDbInfo = relation.getKey().split(CdcConstants.ESCAPE_DOT);
            String sourceDbName = sourceDbInfo[0];
            String sourceDbTable = sourceDbInfo[1];
            if (dbIndexs.get(sourceDbName) == null) {
                String dbIndex = DbUtil.getDbShardingIndex(sourceDbName);
                dbIndexs.put(sourceDbName, dbIndex);
            }
            String tableIndex = DbUtil.getTableShardingIndex(sourceDbTable);
            tableIndexs.put(sourceDbName, sourceDbTable, tableIndex);
        }
    }

}