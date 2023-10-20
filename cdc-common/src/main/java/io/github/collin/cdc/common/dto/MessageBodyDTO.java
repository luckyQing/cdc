package io.github.collin.cdc.common.dto;

import io.github.collin.cdc.common.enums.OpType;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.Map;

/**
 * 增量数据发送给mq的消息body
 *
 * @author collin
 * @date 2023-08-16
 */
@Getter
@Setter
@ToString
public class MessageBodyDTO {

    /**
     * change log类型
     *
     * @see OpType
     */
    private byte op;
    /**
     * 表数据（json字节数组格式）
     */
    private Map<String, Object> row;

}