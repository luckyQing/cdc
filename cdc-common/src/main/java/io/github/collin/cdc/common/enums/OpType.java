package io.github.collin.cdc.common.enums;

import io.github.collin.cdc.common.exception.ConvertRowKindException;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.flink.types.RowKind;

/**
 * 操作类型
 *
 * @author collin
 * @date 2023-04-24
 */
@Getter
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public enum OpType {

    /**
     * 插入
     * @see RowKind#INSERT
     */
    INSERT((byte) 0),

    /**
     * 更新前数据
     * @see RowKind#UPDATE_BEFORE
     */
    UPDATE_BEFORE((byte) 1),

    /**
     * 更新后数据
     * @see RowKind#UPDATE_AFTER
     */
    UPDATE_AFTER((byte) 2),

    /**
     * 删除数据
     * @see RowKind#DELETE
     */
    DELETE((byte) 3),
    /**
     * ddl语句
     */
    DDL((byte) 4);

    private byte type;

    /**
     * 转换成rowkind
     *
     * @return
     */
    public static RowKind convertRowKind(OpType type) {
        if (type == INSERT) {
            return RowKind.INSERT;
        }
        if (type == UPDATE_BEFORE) {
            return RowKind.UPDATE_BEFORE;
        }
        if (type == UPDATE_AFTER) {
            return RowKind.UPDATE_AFTER;
        }
        if (type == DELETE) {
            return RowKind.DELETE;
        }

        throw new ConvertRowKindException(String.format("type=%s", type));
    }

}