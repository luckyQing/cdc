package io.github.collin.cdc.mysql.cdc.iceberg.source;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateUtil;
import io.github.collin.cdc.common.constants.SchemaConstants;
import io.github.collin.cdc.common.enums.OpType;
import io.github.collin.cdc.common.util.JacksonUtil;
import io.github.collin.cdc.mysql.cdc.common.dto.RowJson;
import io.github.collin.cdc.mysql.cdc.common.properties.FlinkDatasourceProperties;
import io.github.collin.cdc.mysql.cdc.common.util.DbCommonUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 修复数据source
 *
 * @author collin
 * @date 2025-01-09
 */
@Slf4j
@RequiredArgsConstructor
public class FixDataMysqlSource extends RichSourceFunction<RowJson> implements CheckpointListener {

    private final FlinkDatasourceProperties datasourceProperties;
    private final String sourceDbName;
    private final List<String> sourceTableNames;
    private final String fixDataSql;
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern(DatePattern.NORM_DATETIME_PATTERN);

    protected transient Connection connection = null;

    /**
     * 记录同步的表sourceTableNames数组位置，以便任务异常重启后接着上次同步的表继续同步
     */
    private int lastSyncTableIndex = 0;
    private transient int syncTableIndex = 0;

    private long lastOffset = 0;
    private transient long offset = 0;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        String url = DbCommonUtil.buildUrl(datasourceProperties.getHost(), datasourceProperties.getPort(), sourceDbName, datasourceProperties.getTimeZone());
        this.connection = DbCommonUtil.getConnection(datasourceProperties.getUsername(), datasourceProperties.getPassword(), url);
    }

    @Override
    public void run(SourceContext<RowJson> ctx) throws Exception {
        System.out.println("fix " + JacksonUtil.toJson(sourceTableNames));
        // 接着上次的位置同步
        if (lastSyncTableIndex > 0) {
            syncTableIndex = lastSyncTableIndex;
        }
        if (lastOffset > 0) {
            offset = lastOffset;
        }

        for (; syncTableIndex < sourceTableNames.size(); syncTableIndex++) {
            String sourceTableName = sourceTableNames.get(syncTableIndex);
            System.out.println("prepare fix " + sourceTableName);
            String querySql = String.format(fixDataSql, sourceTableName);
            if (offset > 0) {
                // 从offset开始，直到最后一条记录
                querySql = querySql + " limit " + offset + ",-1";
            }
            try (PreparedStatement pst = connection.prepareStatement(querySql);
                 ResultSet resultSet = pst.executeQuery()) {
                System.out.println("start fix " + sourceTableName);

                // 设置每次获取的数据行数
                resultSet.setFetchSize(20000);

                ResultSetMetaData metaData = resultSet.getMetaData();
                int columnCount = metaData.getColumnCount();
                // 流式处理结果集
                while (resultSet.next()) {
                    // 表数据
                    Map<String, Object> json = new HashMap<>();
                    for (int i = 1; i <= columnCount; i++) {
                        Object columnValue = resultSet.getObject(i);
                        if (columnValue != null) {
                            int columnType = metaData.getColumnType(i);
                            if (columnType == Types.TIMESTAMP_WITH_TIMEZONE) {
                                Timestamp timestamp = (Timestamp) columnValue;
                                columnValue = timestamp.toLocalDateTime().format(DATE_TIME_FORMATTER);
                            } else if (columnType == Types.TIMESTAMP) {
                                if (columnValue instanceof Timestamp) {
                                    Timestamp timestamp = (Timestamp) columnValue;
                                    columnValue = timestamp.toLocalDateTime().format(DATE_TIME_FORMATTER);
                                } else {
                                    LocalDateTime dateTime = (LocalDateTime) columnValue;
                                    columnValue = dateTime.format(DATE_TIME_FORMATTER);
                                }
                            } else if (columnType == Types.DATE) {
                                if (columnValue != null) {
                                    columnValue = DateUtil.formatDate((Date) columnValue);
                                }
                            } else if (columnType == Types.BIT) {
                                columnValue = resultSet.getInt(i);
                            }
                        }

                        json.put(metaData.getColumnName(i), columnValue);
                    }
                    json.put(SchemaConstants.SYNC_TS, System.currentTimeMillis());

                    RowJson element = new RowJson();
                    element.setDb(sourceDbName);
                    element.setTable(sourceTableName);
                    element.setOp(OpType.INSERT);
                    element.setJson(json);
                    element.setIncremental(true);

                    ctx.collect(element);
                    offset++;
                }
                System.out.println("end fix " + sourceTableName);
                offset = 0;
            }
        }
    }

    @Override
    public void cancel() {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                log.error("colse mysql connection error", e);
            }
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        // 记录同步的表查询位置
        this.lastSyncTableIndex = syncTableIndex;
        this.lastOffset = offset;
    }

}