package io.github.collin.cdc.mysql.cdc.mysql.util;

import io.github.collin.cdc.common.util.RedisKeyUtil;
import io.github.collin.cdc.mysql.cdc.common.constants.DbConstants;
import io.github.collin.cdc.mysql.cdc.common.util.DbCommonUtil;
import io.github.collin.cdc.mysql.cdc.mysql.constants.JdbcConstants;
import io.github.collin.cdc.mysql.cdc.mysql.dto.TableDTO;
import io.github.collin.cdc.mysql.cdc.mysql.properties.DatasourceCdcProperties;
import io.github.collin.cdc.mysql.cdc.mysql.properties.DatasourceProperties;
import io.github.collin.cdc.mysql.cdc.mysql.properties.DatasourceRuleProperties;
import io.github.collin.cdc.mysql.cdc.mysql.properties.DatasourceShardingTableProperties;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.util.Preconditions;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Pattern;

/**
 * 数据库操作工具类
 *
 * @author collin
 * @date 2019-07-13
 */
public class DbUtil extends DbCommonUtil {

    /**
     * 获取表信息
     *
     * @param connection
     * @param type
     * @param tables
     * @return
     * @throws SQLException
     */
    public static Set<String> getTables(Connection connection, int type, Set<String> tables, Map<String, DatasourceShardingTableProperties> shardingTables, boolean isFilterNodataTable)
            throws SQLException {
        Set<String> tableList = new HashSet<>(16);
        try (ResultSet resultSet = connection.getMetaData().getTables(connection.getCatalog(), DbConstants.PUBLIC_SCHEMA_PATTERN, null, new String[]{DbConstants.TABLE_TYPE})) {
            while (resultSet.next()) {
                String tableName = resultSet.getString(3);
                if (matchSharding(tableName, shardingTables)) {
                    continue;
                }
                if (filterTable(type, tables, tableName)) {
                    continue;
                }

                tableList.add(tableName);
            }
        }

        if (shardingTables != null && !shardingTables.isEmpty()) {
            for (Map.Entry<String, DatasourceShardingTableProperties> entry : shardingTables.entrySet()) {
                if (isFilterNodataTable && entry.getValue().isNodata()) {
                    continue;
                }
                tableList.add(entry.getValue().getSourceTable());
            }
        }

        return tableList;
    }

    private static boolean matchSharding(String tableName, Map<String, DatasourceShardingTableProperties> shardingTables) {
        if (shardingTables == null || shardingTables.isEmpty()) {
            return false;
        }

        Collection<DatasourceShardingTableProperties> tableNameSources = shardingTables.values();
        for (DatasourceShardingTableProperties tableNameSource : tableNameSources) {
            Pattern pattern = Pattern.compile(tableNameSource.getSourceTable());
            if (pattern.matcher(tableName).matches()) {
                return true;
            }
        }

        return false;
    }

    public static Map<String, TableDTO> listAvailableTables(DatasourceCdcProperties datasourceCdcProperties, Map<String, DatasourceRuleProperties> details, String timeZone) {
        DatasourceProperties source = datasourceCdcProperties.getSource();
        List<String> allDatabases = DbUtil.listDatabases(source.getHost(), source.getPort(), source.getUsername(),
                source.getPassword(), timeZone);
        Map<String, TableDTO> tableRelations = new HashMap<>();
        for (String name : allDatabases) {
            boolean match = false;
            DatasourceRuleProperties detailProperties = null;
            for (Map.Entry<String, DatasourceRuleProperties> entry : details.entrySet()) {
                DatasourceRuleProperties value = entry.getValue();
                String dbNameSource = value.getSharding().getSourceDb();
                if (StringUtils.isNotBlank(dbNameSource)) {
                    Pattern pattern = Pattern.compile(dbNameSource);
                    if (pattern.matcher(name).matches()) {
                        match = true;
                    }
                }

                if (entry.getKey().equals(name)) {
                    match = true;
                }
                if (match) {
                    detailProperties = value;
                    break;
                }
            }
            if (match) {
                String url = DbUtil.buildUrl(source.getHost(), source.getPort(), name, timeZone);
                Map<String, DatasourceShardingTableProperties> shardingTables = detailProperties.getSharding().getTables();
                boolean isSharding = shardingTables != null && !shardingTables.isEmpty();
                String targetDbName = StringUtils.isNotBlank(detailProperties.getSharding().getTargetDb()) ? detailProperties.getSharding().getTargetDb() : name;
                try (Connection connection = DbUtil.getConnection(source.getUsername(), source.getPassword(), url);
                     ResultSet resultSet = connection.getMetaData().getTables(connection.getCatalog(), DbConstants.PUBLIC_SCHEMA_PATTERN, null, new String[]{DbConstants.TABLE_TYPE})) {
                    while (resultSet.next()) {
                        String tableName = resultSet.getString(3);

                        if (isSharding) {
                            boolean matchSharding = false;
                            for (Map.Entry<String, DatasourceShardingTableProperties> entry : shardingTables.entrySet()) {
                                DatasourceShardingTableProperties shardingTableProperties = entry.getValue();
                                Pattern pattern = Pattern.compile(shardingTableProperties.getSourceTable());
                                if (pattern.matcher(tableName).matches()) {
                                    String key = RedisKeyUtil.buildTableRelationKey(name, tableName);
                                    Preconditions.checkState(!tableRelations.containsKey(key), String.format("tableRelations[%s] exists!", key));
                                    tableRelations.put(key, new TableDTO(targetDbName, entry.getKey(), shardingTableProperties.isSharding(), shardingTableProperties.getShardingType(),
                                            shardingTableProperties.getShardingFieldName(), shardingTableProperties.getBatchSize(), shardingTableProperties.isNodata()));
                                    matchSharding = true;
                                    break;
                                }
                            }
                            if (matchSharding) {
                                continue;
                            }
                        }

                        if (filterTable(detailProperties.getType(), detailProperties.getTables(), tableName)) {
                            continue;
                        }

                        String key = RedisKeyUtil.buildTableRelationKey(name, tableName);
                        Preconditions.checkState(!tableRelations.containsKey(key), String.format("tableRelations[%s] exists!", key));
                        tableRelations.put(key, new TableDTO(targetDbName, tableName, false, null,
                                null, JdbcConstants.BATCH_SIZE, false));
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }

        // 按key倒叙排序，以便建表时，根据最新的表创建
        List<String> keys = new ArrayList<>(tableRelations.keySet());
        Collections.sort(keys, Collections.reverseOrder());

        Map<String, TableDTO> result = new LinkedHashMap<>();
        for (String key : keys) {
            result.put(key, tableRelations.get(key));
        }

        return result;
    }

}