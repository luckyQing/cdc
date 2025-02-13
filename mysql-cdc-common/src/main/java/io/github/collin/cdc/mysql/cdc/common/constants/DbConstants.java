package io.github.collin.cdc.mysql.cdc.common.constants;

/**
 * 数据库相关常量
 *
 * @author collin
 * @date 2021-12-12
 */
public class DbConstants {

    /**
     * jdbc查询表的类型
     */
    public static final String TABLE_TYPE = "TABLE";

    /**
     * “PUBLIC” schema
     */
    public static final String PUBLIC_SCHEMA_PATTERN = "PUBLIC";
    /**
     * mysql分隔符
     */
    public static final String MYSQL_DELIMITER = "`";

    /**
     * 数据库连接属性
     *
     * @author collin
     * @date 2021-12-12
     */
    public static class ConnectionProperties {
        /**
         * 数据库用户名
         */
        public static final String USER = "user";
        /**
         * 数据库密码
         */
        public static final String PASSWORD = "password";
        /**
         * 获取Oracle元数据 REMARKS信息
         */
        public static final String REMARKS_REPORTING = "remarksReporting";
        /**
         * 获取MySQL元数据 REMARKS信息
         */
        public static final String USE_INFORMATION_SCHEMA = "useInformationSchema";

        private ConnectionProperties() {
        }
    }

    private DbConstants() {
    }

}