package io.github.collin.cdc.migration.mysql.util;

import com.mysql.cj.jdbc.Driver;
import io.github.collin.cdc.common.util.RedisKeyUtil;
import io.github.collin.cdc.migration.mysql.constants.DbConstants;
import io.github.collin.cdc.migration.mysql.constants.JdbcConstants;
import io.github.collin.cdc.migration.mysql.dto.ColumnMetaDataDTO;
import io.github.collin.cdc.migration.mysql.dto.TableDTO;
import io.github.collin.cdc.migration.mysql.dto.TableMetaDataDTO;
import io.github.collin.cdc.migration.mysql.enums.SyncType;
import io.github.collin.cdc.migration.mysql.properties.DatasourceCdcProperties;
import io.github.collin.cdc.migration.mysql.properties.DatasourceProperties;
import io.github.collin.cdc.migration.mysql.properties.DatasourceRuleProperties;
import io.github.collin.cdc.migration.mysql.properties.DatasourceShardingTableProperties;
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


    /**
     * 获取数据库连接
     *
     * @param datasourceProperties
     * @param dbName
     * @param serverTimezone
     * @return
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    public static Connection getConnection(DatasourceProperties datasourceProperties, String dbName, String serverTimezone) throws ClassNotFoundException, SQLException {
        Class.forName(Driver.class.getName());

        Properties props = new Properties();
        props.setProperty(DbConstants.ConnectionProperties.USER, datasourceProperties.getUsername());
        props.setProperty(DbConstants.ConnectionProperties.PASSWORD, datasourceProperties.getPassword());
        // 获取Oracle元数据 REMARKS信息
        props.setProperty(DbConstants.ConnectionProperties.REMARKS_REPORTING, "true");
        // 获取MySQL元数据 REMARKS信息
        props.setProperty(DbConstants.ConnectionProperties.USE_INFORMATION_SCHEMA, "true");
        String url = buildUrl(datasourceProperties.getHost(), datasourceProperties.getPort(), dbName, serverTimezone);
        return DriverManager.getConnection(url, props);
    }

    /**
     * 构建url
     *
     * @param host
     * @param port
     * @param dbName
     * @param serverTimezone
     * @return
     */
    public static String buildUrl(String host, int port, String dbName, String serverTimezone) {
        String url = null;
        try {
            url = String.format("jdbc:mysql://%s:%s/%s?characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&allowMultiQueries=true&useSSL=false&rewriteBatchedStatements=true&serverTimezone=%s",
                    host, port, dbName, URLEncoder.encode(serverTimezone, StandardCharsets.UTF_8.name()));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        return url;
    }

    /**
     * 获取表信息
     *
     * @param connnection
     * @param type
     * @param tables
     * @param shardingTables
     * @return
     * @throws SQLException
     */
    public static Map<String, TableMetaDataDTO> getTablesMetaData(Connection connnection, int type, Set<String> tables, Map<String, DatasourceShardingTableProperties> shardingTables)
            throws SQLException {
        Map<String, TableMetaDataDTO> tableMetaDataMap = new HashMap<>(16);
        try (ResultSet resultSet = connnection.getMetaData().getTables(connnection.getCatalog(), DbConstants.PUBLIC_SCHEMA_PATTERN, null, new String[]{DbConstants.TABLE_TYPE})) {
            while (resultSet.next()) {
                String tableName = resultSet.getString(3);
                // TODO:
                if (matchSharding(tableName, shardingTables)) {
                    continue;
                }
                if (filterTable(type, tables, tableName)) {
                    continue;
                }

                TableMetaDataDTO tableMetaDataDTO = new TableMetaDataDTO();
                tableMetaDataDTO.setName(tableName);
                tableMetaDataDTO.setComment(resultSet.getString(5));

                tableMetaDataMap.put(tableMetaDataDTO.getName(), tableMetaDataDTO);
            }
        }
        return tableMetaDataMap;
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

    /**
     * 获取表字段信息（分库分表的列多了2个字段“数据库名、表名”，由“数据库名、表名、原id”共同组成主键）
     *
     * @param connnection
     * @param database
     * @param tableName
     * @return
     * @throws SQLException
     */
    public static List<ColumnMetaDataDTO> getTableColumnMetaDatas(Connection connnection, String database, String tableName) throws SQLException {
        DatabaseMetaData metaData = connnection.getMetaData();
        // 主键
        Set<String> primaryKeyColumnNames = getPrimaryKeyColumnName(metaData, database, tableName);

        List<ColumnMetaDataDTO> columnMetaDatas = new ArrayList<>();
        try (ResultSet columnsResultSet = metaData.getColumns(database, null, tableName, null)) {
            while (columnsResultSet.next()) {
                ColumnMetaDataDTO columnMetaData = new ColumnMetaDataDTO();
                String name = columnsResultSet.getString(4);
                columnMetaData.setName(name);
                columnMetaData.setPrimaryKey(primaryKeyColumnNames.contains(name));
                columnMetaDatas.add(columnMetaData);
            }
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
    private static Set<String> getPrimaryKeyColumnName(DatabaseMetaData metaData, String database,
                                                       String tableName) throws SQLException {
        Set<String> primaryKeys = new HashSet<>();
        try (ResultSet primaryKeyResultSet = metaData.getPrimaryKeys(database, null, tableName)) {
            while (primaryKeyResultSet.next()) {
                primaryKeys.add(primaryKeyResultSet.getString(4));
            }
        }
        return primaryKeys;
    }

    public static Map<String, TableDTO> listAvailableTables(DatasourceCdcProperties datasourceCdcProperties, Map<String, DatasourceRuleProperties> details, String timeZone) {
        DatasourceProperties source = datasourceCdcProperties.getSource();
        Set<String> allDatabases = DbUtil.listDatabases(source, timeZone);
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
                                    tableRelations.put(key, new TableDTO(targetDbName, entry.getKey(), true, shardingTableProperties.getShardingType(),
                                            shardingTableProperties.getFieldUidName(), shardingTableProperties.getBatchSize(), shardingTableProperties.isNodata()));
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

        return tableRelations;
    }

    /**
     * 获取所有可用的数据库
     *
     * @param datasourceCdcProperties
     * @param details
     * @param timeZone
     * @return
     */
    public static Map<String, String> listAvailableDatabases(DatasourceCdcProperties datasourceCdcProperties, Map<String, DatasourceRuleProperties> details, String timeZone) {
        Set<String> allDatabases = DbUtil.listDatabases(datasourceCdcProperties.getSource(), timeZone);
        Map<String, String> dbRelations = new HashMap<>();
        for (String name : allDatabases) {
            for (Map.Entry<String, DatasourceRuleProperties> entry : details.entrySet()) {
                DatasourceRuleProperties detailProperties = entry.getValue();
                String dbNameSource = detailProperties.getSharding().getSourceDb();
                if (StringUtils.isNotBlank(dbNameSource)) {
                    Pattern pattern = Pattern.compile(dbNameSource);
                    if (pattern.matcher(name).matches()) {
                        Preconditions.checkState(!dbRelations.containsKey(name), String.format("database[%s] exists!", name));
                        dbRelations.put(name, detailProperties.getSharding().getTargetDb());
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

    private static Set<String> listDatabases(DatasourceProperties datasourceProperties, String timeZone) {
        String url = DbUtil.buildUrl(datasourceProperties.getHost(), datasourceProperties.getPort(), DbConstants.INFORMATION_SCHEMA_DBNAME, timeZone);
        try (Connection connection = DbUtil.getConnection(datasourceProperties.getUsername(), datasourceProperties.getPassword(), url)) {
            return DbUtil.listDatabases(connection);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 获取当前连接的所有数据库的名称
     *
     * @param connnection
     * @return
     */
    public static Set<String> listDatabases(Connection connnection) {
        Set<String> databases = new HashSet<>();
        try (ResultSet resultSet = connnection.getMetaData().getCatalogs()) {
            while (resultSet.next()) {
                databases.add(resultSet.getString(1));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return databases;
    }

    /**
     * 获取创建数据库的sql
     *
     * @param connnection
     * @return
     */
    public static String getCreateDatabaseSql(Connection connnection, String database) {
        try (PreparedStatement preparedStatement = connnection.prepareStatement("show create database " + database);
             ResultSet resultSet = preparedStatement.executeQuery()) {
            while (resultSet.next()) {
                return resultSet.getString(2);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        throw new IllegalArgumentException(String.format("database[%s] is not exist", database));
    }

    /**
     * 获取创建表的sql
     *
     * @param connnection
     * @return
     */
    public static String getCreateTableSql(Connection connnection, String table) {
        try (PreparedStatement preparedStatement = connnection.prepareStatement("show create table " + table);
             ResultSet resultSet = preparedStatement.executeQuery()) {
            while (resultSet.next()) {
                return resultSet.getString(2);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        throw new IllegalArgumentException(String.format("database[%s] is not exist", table));
    }

    /**
     * 执行sql
     *
     * @param connnection
     * @return
     */
    public static boolean executeSql(Connection connnection, String sql) {
        try (PreparedStatement preparedStatement = connnection.prepareStatement(sql)) {
            return preparedStatement.execute();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 表是否存在
     *
     * @param connnection
     * @return
     */
    public static boolean isTableExists(Connection connnection, String dbName, String tableName) {
        String sql = "SELECT COUNT(*) FROM information_schema.tables WHERE table_name=? AND table_schema=?";
        try (PreparedStatement preparedStatement = connnection.prepareStatement(sql)) {
            preparedStatement.setString(1, tableName);
            preparedStatement.setString(2, dbName);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    return resultSet.getInt(1) > 0;
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return false;
    }

    /**
     * 获取分库索引
     *
     * @param dbName
     * @return
     */
    public static String getDbShardingIndex(String dbName) {
        Integer firstNumberIndex = null, lastNumberIndex = null;
        for (int i = 0; i < dbName.length(); i++) {
            char c = dbName.charAt(i);
            if (c >= '0' && c <= '9') {
                if (firstNumberIndex == null) {
                    firstNumberIndex = i;
                }
                lastNumberIndex = i;
            }
        }
        return dbName.substring(firstNumberIndex, lastNumberIndex + 1);
    }

    /**
     * 获取分表索引
     *
     * @param tableName
     * @return
     */
    public static String getTableShardingIndex(String tableName) {
        Integer firstNumberIndex = null;
        for (int i = 0; i < tableName.length(); i++) {
            char c = tableName.charAt(i);
            if (c >= '0' && c <= '9') {
                firstNumberIndex = i;
                break;
            }
        }
        return tableName.substring(firstNumberIndex);
    }

}