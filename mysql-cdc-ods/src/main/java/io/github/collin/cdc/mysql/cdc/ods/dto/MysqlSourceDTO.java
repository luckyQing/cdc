package io.github.collin.cdc.mysql.cdc.ods.dto;

import lombok.*;

/**
 * mysql源配置
 *
 * @author collin
 * @date 2023-04-24
 */
@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class MysqlSourceDTO {

    /**
     * 源数据库列表
     */
    private String[] databaseList;

    /**
     * 源表列表
     */
    private String[] tableList;

}