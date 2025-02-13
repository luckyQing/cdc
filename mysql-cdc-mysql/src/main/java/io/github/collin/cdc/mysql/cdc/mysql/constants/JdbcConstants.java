package io.github.collin.cdc.mysql.cdc.mysql.constants;

public interface JdbcConstants {

    /**
     * jdbc批量提交大小
     */
    int BATCH_SIZE = 1024;
    /**
     * 批量提交间隔时间
     */
    long BATCH_INTERVAL_MS = 3000L;

}