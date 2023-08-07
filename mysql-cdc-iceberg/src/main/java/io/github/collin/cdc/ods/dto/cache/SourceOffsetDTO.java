package io.github.collin.cdc.ods.dto.cache;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

@Getter
@Setter
@ToString
public class SourceOffsetDTO implements Serializable {
    private static final long serialVersionUID = 1L;

    private String filename;
    private long position;
    /**
     * 数据库实例名
     */
    private String instanceName;

}