package io.github.collin.cdc.common.util;

import io.github.collin.cdc.common.constants.CdcConstants;

/**
 * 缓存工具类
 *
 * @author collin
 * @date 2023-05-06
 */
public class RedisKeyUtil {

    public static final String buildTableRelationKey(String sourceDbName, String sourceTableName) {
        return new StringBuilder(32).append(sourceDbName).append(".").append(sourceTableName).toString();
    }

    public static final String buildDdlStateSetKey() {
        return new StringBuilder(CdcConstants.REDIS_CACHE_PREFIX_KEY).append("ddlstate").toString();
    }

    public static final String buildDdlStateContentKey(String file, long position) {
        return new StringBuilder().append(file).append(":").append(position).toString();
    }

    public static final String buildDdlKey(String application) {
        return new StringBuilder(CdcConstants.REDIS_DATA_PREFIX_KEY).append("ddl:").append(application).toString();
    }

    public static final String buildApplicationKey() {
        return new StringBuilder(CdcConstants.REDIS_DATA_PREFIX_KEY).append("application").toString();
    }

}