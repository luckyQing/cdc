package io.github.collin.cdc.mysql.cdc.ods.properties;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

@Getter
@Setter
@ToString
public class SinkTypeProperties implements Serializable {

    /**
     * sink端类型
     *
     * @see io.github.collin.cdc.mysql.cdc.ods.enums.SinkType
     */
    private String sinkType;

}