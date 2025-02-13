package io.github.collin.cdc.mysql.cdc.mysql.util;

import io.github.collin.cdc.common.constants.CdcConstants;

/**
 * redis key工具类
 *
 * @author collin
 * @date 2023-07-20
 */
public class MigrationRedisKeyUtil {

    public static final String buildConfigKey() {
        return CdcConstants.REDIS_DATA_PREFIX_KEY + "migration:config";
    }

    public static final String buildConfigHashKey(String instanceName) {
        return instanceName;
    }

}