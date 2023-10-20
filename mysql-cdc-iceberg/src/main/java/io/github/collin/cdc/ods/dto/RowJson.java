package io.github.collin.cdc.ods.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.github.collin.cdc.common.enums.OpType;
import lombok.*;

import java.io.Serializable;
import java.util.Map;

/**
 * 提取出 [db、table、rowKind、data 对应结果的 json] 序列化结果
 *
 * @author collin
 * @date 2023-04-24
 */
@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class RowJson implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 源数据库名
     */
    private String db;
    /**
     * 源表名
     */
    private String table;
    /**
     * change log类型
     *
     * @see OpType
     */
    private byte op;
    /**
     * 表数据
     */
    private Map<String, Object> json;
    /**
     * ddl语句
     */
    private String ddl;
    /**
     * binlog信息
     */
    private String offset;
    /**
     * 是否是增量数据
     */
    private boolean incremental;

}