package io.github.collin.cdc.common.util;

import io.github.collin.cdc.common.enums.Env;

/**
 * 新增mq工具类
 *
 * @author collin
 * @date 2023-08-21
 */
public class MqUtil {

    /**
     * 获取mq topic
     *
     * @param dbName    目标库名
     * @param tableName 目标表名
     * @return
     */
    public static String getTopic(String dbName, String tableName) {
        return getTopic(Env.PROD.name(), dbName, tableName);
    }

    /**
     * 获取mq topic
     *
     * @param e         环境
     * @param dbName    目标库名
     * @param tableName 目标表名
     * @return
     */
    public static String getTopic(String e, String dbName, String tableName) {
        Env env = Env.valueOf(e);
        if (env == Env.PROD) {
            return String.format("flink.%s.%s", dbName, tableName);
        }

        return String.format("flink.%s.%s.%s", e.toLowerCase(), dbName, tableName);
    }

}