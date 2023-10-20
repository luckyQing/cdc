package io.github.collin.cdc.common.exception;

import org.apache.flink.types.RowKind;

/**
 * 转换{@link RowKind}失败异常
 *
 * @author collin
 * @date 2023-04-24
 */
public class ConvertRowKindException extends RuntimeException {

    public ConvertRowKindException(String message) {
        super(message);
    }

}