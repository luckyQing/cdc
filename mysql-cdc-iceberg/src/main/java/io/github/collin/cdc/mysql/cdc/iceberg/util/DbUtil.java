package io.github.collin.cdc.mysql.cdc.iceberg.util;

import com.mysql.cj.MysqlType;
import com.mysql.cj.jdbc.Driver;
import io.github.collin.cdc.common.util.RedisKeyUtil;
import io.github.collin.cdc.mysql.cdc.iceberg.constants.DbConstants;
import io.github.collin.cdc.mysql.cdc.iceberg.enums.SyncType;
import io.github.collin.cdc.mysql.cdc.iceberg.properties.FlinkDatasourceDetailProperties;
import io.github.collin.cdc.mysql.cdc.iceberg.properties.FlinkDatasourceProperties;
import io.github.collin.cdc.mysql.cdc.iceberg.dto.ColumnMetaDataDTO;
import io.github.collin.cdc.mysql.cdc.iceberg.dto.TableDTO;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.util.Preconditions;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.regex.Pattern;

/**
 * 数据库操作工具类
 *
 * @author collin
 * @date 2019-07-13
 */
public class DbUtil {

    private DbUtil() {
    }

    /**
     * 获取数据库连接
     *
     * @param username
     * @param password
     * @param url
     * @return
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    public static Connection getConnection(String username, String password, String url) throws ClassNotFoundException, SQLException {
        Class.forName(Driver.class.getName());

        Properties props = new Properties();
        props.setProperty(DbConstants.ConnectionProperties.USER, username);
        props.setProperty(DbConstants.ConnectionProperties.PASSWORD, password);
        // 获取Oracle元数据 REMARKS信息
        props.setProperty(DbConstants.ConnectionProperties.REMARKS_REPORTING, "true");
        // 获取MySQL元数据 REMARKS信息
        props.setProperty(DbConstants.ConnectionProperties.USE_INFORMATION_SCHEMA, "true");
        return DriverManager.getConnection(url, props);
    }

    public static String buildUrl(String host, int port, String dbName, String serverTimezone) {
        String url = null;
        try {
            url = String.format("jdbc:mysql://%s:%s/%s?characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&allowMultiQueries=true&useSSL=false&serverTimezone=%s",
                    host, port, dbName, URLEncoder.encode(serverTimezone, StandardCharsets.UTF_8.name()));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        return url;
    }

    /**
     * 获取表信息
     *
     * @param connection
     * @param type
     * @param tables
     * @return
     * @throws SQLException
     */
    public static Set<String> getTables(Connection connection, int type, Set<String> tables, Map<String, String> shardingTables)
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
            tableList.addAll(shardingTables.values());
        }

        return tableList;
    }

    private static boolean filterTable(int type, Set<String> tables, String tableName) {
        if (SyncType.ALL.getType() == type) {
            return false;
        }

        if (tables == null) {
            return true;
        }

        if (SyncType.INCLUDE.getType() == type) {
            return !tables.contains(tableName);
        }

        if (SyncType.EXCLUDE.getType() == type) {
            return tables.contains(tableName);
        }

        return true;
    }

    private static boolean matchSharding(String tableName, Map<String, String> shardingTables) {
        if (shardingTables == null || shardingTables.isEmpty()) {
            return false;
        }

        Collection<String> tableNameSources = shardingTables.values();
        for (String tableNameSource : tableNameSources) {
            Pattern pattern = Pattern.compile(tableNameSource);
            if (pattern.matcher(tableName).matches()) {
                return true;
            }
        }

        return false;
    }

    /**
     * 获取表字段信息（分库分表的列多了2个字段“数据库名、表名”，由“数据库名、表名、原id”共同组成主键）
     *
     * @param connection
     * @param database
     * @param tableName
     * @return
     * @throws SQLException
     */
    public static List<ColumnMetaDataDTO> getTableColumnMetaDatas(Connection connection, String database, String tableName) throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
        List<ColumnMetaDataDTO> columnMetaDatas = new ArrayList<>();
        try (ResultSet columnsResultSet = metaData.getColumns(database, null, tableName, null)) {
            while (columnsResultSet.next()) {
                ColumnMetaDataDTO columnMetaData = new ColumnMetaDataDTO();
                columnMetaData.setName(columnsResultSet.getString(4));
                columnMetaData.setComment(columnsResultSet.getString(12));
                columnMetaData.setMysqlType(MysqlType.getByName(columnsResultSet.getString(6)));
                columnMetaData.setLength(columnsResultSet.getInt(7));
                columnMetaData.setPrimaryKey(false);
                columnMetaDatas.add(columnMetaData);
            }
        }

        // 主键
        Set<String> primaryKeyColumnNames = getPrimaryKeyColumnNames(metaData, database, tableName);
        if (CollectionUtils.isNotEmpty(primaryKeyColumnNames)) {
            columnMetaDatas.stream()
                    .filter(columnMetaData -> primaryKeyColumnNames.contains(columnMetaData.getName()))
                    .forEach(columnMetaData -> columnMetaData.setPrimaryKey(true));
        }

        return columnMetaDatas;
    }

    /**
     * 获取主键字段名
     *
     * @param metaData
     * @param database
     * @param tableName
     * @return 值为null则表示没有主键
     * @throws SQLException
     */
    private static Set<String> getPrimaryKeyColumnNames(DatabaseMetaData metaData, String database,
                                                        String tableName) throws SQLException {
        Set<String> primaryKeys = new HashSet<>();
        try (ResultSet primaryKeyResultSet = metaData.getPrimaryKeys(database, null, tableName)) {
            while (primaryKeyResultSet.next()) {
                primaryKeys.add(primaryKeyResultSet.getString(4));
            }
        }
        return primaryKeys;
    }

    public static Map<String, TableDTO> listAvailableTables(FlinkDatasourceProperties datasourceProperties, Map<String, FlinkDatasourceDetailProperties> details) {
        List<String> allDatabases = DbUtil.listDatabases(datasourceProperties);
        Map<String, TableDTO> tableRelations = new HashMap<>();
        for (String name : allDatabases) {
            boolean match = false;
            FlinkDatasourceDetailProperties detailProperties = null;
            for (Map.Entry<String, FlinkDatasourceDetailProperties> entry : details.entrySet()) {
                FlinkDatasourceDetailProperties value = entry.getValue();
                String dbNameSource = value.getSharding().getDbNameSource();
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
                String url = DbUtil.buildUrl(datasourceProperties.getHost(), datasourceProperties.getPort(), name, datasourceProperties.getTimeZone());
                Map<String, String> shardingTables = detailProperties.getSharding().getTables();
                boolean isSharding = shardingTables != null && !shardingTables.isEmpty();
                String targetDbName = StringUtils.isNotBlank(detailProperties.getSharding().getDbNameTarget()) ? detailProperties.getSharding().getDbNameTarget() : name;
                try (Connection connection = DbUtil.getConnection(datasourceProperties.getUsername(), datasourceProperties.getPassword(), url);
                     ResultSet resultSet = connection.getMetaData().getTables(connection.getCatalog(), DbConstants.PUBLIC_SCHEMA_PATTERN, null, new String[]{DbConstants.TABLE_TYPE})) {
                    while (resultSet.next()) {
                        String tableName = resultSet.getString(3);

                        if (isSharding) {
                            boolean matchSharding = false;
                            for (Map.Entry<String, String> entry : shardingTables.entrySet()) {
                                Pattern pattern = Pattern.compile(entry.getValue());
                                if (pattern.matcher(tableName).matches()) {
                                    String key = RedisKeyUtil.buildTableRelationKey(name, tableName);
                                    Preconditions.checkState(!tableRelations.containsKey(key), String.format("tableRelations[%s] exists!", key));
                                    tableRelations.put(key, new TableDTO(targetDbName, entry.getKey(), true));
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
                        tableRelations.put(key, new TableDTO(targetDbName, tableName, false));
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

    /**
     * 获取所有可用的数据库
     *
     * @param datasourceProperties
     * @param details
     * @return
     */
    public static Map<String, String> listAvailableDatabases(FlinkDatasourceProperties datasourceProperties, Map<String, FlinkDatasourceDetailProperties> details) {
        List<String> allDatabases = DbUtil.listDatabases(datasourceProperties);
        Map<String, String> dbRelations = new HashMap<>();
        for (String name : allDatabases) {
            for (Map.Entry<String, FlinkDatasourceDetailProperties> entry : details.entrySet()) {
                FlinkDatasourceDetailProperties detailProperties = entry.getValue();
                String dbNameSource = detailProperties.getSharding().getDbNameSource();
                if (StringUtils.isNotBlank(dbNameSource)) {
                    Pattern pattern = Pattern.compile(dbNameSource);
                    if (pattern.matcher(name).matches()) {
                        Preconditions.checkState(!dbRelations.containsKey(name), String.format("database[%s] exists!", name));
                        dbRelations.put(name, detailProperties.getSharding().getDbNameTarget());
                        break;
                    }
                }

                if (entry.getKey().equals(name)) {
                    Preconditions.checkState(!dbRelations.containsKey(name), String.format("database[%s] exists!", name));
                    dbRelations.put(name, entry.getKey());
                    break;
                }
            }
        }

        return dbRelations;
    }

    private static List<String> listDatabases(FlinkDatasourceProperties datasourceProperties) {
        String url = DbUtil.buildUrl(datasourceProperties.getHost(), datasourceProperties.getPort(), datasourceProperties.getOneDatabaseName(), datasourceProperties.getTimeZone());
        try (Connection connection = DbUtil.getConnection(datasourceProperties.getUsername(), datasourceProperties.getPassword(), url)) {
            return DbUtil.listDatabases(connection);
        } catch (Exception e) {
            throw new RuntimeException(url, e);
        }
    }

    /**
     * 获取当前连接的所有数据库的名称
     *
     * @param connnection
     * @return
     */
    private static List<String> listDatabases(Connection connnection) {
        Set<String> databaseSet = new HashSet<>();
        try (ResultSet resultSet = connnection.getMetaData().getCatalogs();) {
            while (resultSet.next()) {
                databaseSet.add(resultSet.getString(1));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        List<String> databases = new ArrayList<>(databaseSet);
        // 倒叙排序
        Collections.sort(databases, Collections.reverseOrder());
        return databases;
    }

}