package io.github.collin.cdc.ods.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.github.collin.cdc.ods.enums.OpType;
import lombok.*;

import java.io.Serializable;

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
     */
    private OpType op;
    /**
     * 表数据（json字节数组格式）
     */
    private byte[] json;
    /**
     * ddl语句
     */
    private String ddl;
    /**
     * binlog信息
     */
    private String offset;

}