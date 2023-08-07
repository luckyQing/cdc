package io.github.collin.cdc.common.constants;

/**
 * cdc常量
 *
 * @author collin
 * @date 2023-05-06
 */
public class CdcConstants {

    /**
     * 数据库最小连接数
     */
    public static final int DB_CONNECTION_POOL_MIN_SIZE = 32;
    /**
     * 数据库最大连接数
     */
    public static final int DB_CONNECTION_POOL_MAX_SIZE = 128;
    /**
     * ods层数据库前缀
     */
    public static final String NAMESPACE_ODS_PRE = "ods_";
    /**
     * dwd层数据库
     */
    public static final String NAMESPACE_DWD = "dwd";
    /**
     * “.”常量
     */
    public static final String DOT = ".";
    /**
     * 转义“.”常量
     */
    public static final String ESCAPE_DOT = "\\.";
    /**
     * redis数据key前缀
     */
    public static final String REDIS_DATA_PREFIX_KEY = "bigdata:data:";
    /**
     * redis缓存key前缀
     */
    public static final String REDIS_CACHE_PREFIX_KEY = "bigdata:cache:";
    /**
     * redis锁key前缀
     */
    public static final String REDIS_LOCK_PREFIX_KEY = "bigdata:lock:";

}