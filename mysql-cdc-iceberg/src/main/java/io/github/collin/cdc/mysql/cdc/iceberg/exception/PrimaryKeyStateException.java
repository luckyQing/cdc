package io.github.collin.cdc.mysql.cdc.iceberg.exception;

/**
 * 表无主键异常
 *
 * @author collin
 * @date 2023-04-24
 */
public class PrimaryKeyStateException extends RuntimeException {

    public PrimaryKeyStateException(String message) {
        super(message);
    }

}