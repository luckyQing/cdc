package io.github.collin.cdc.mysql.cdc.iceberg.dto.api;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

@Getter
@Setter
@ToString
public class RestartDTO implements Serializable {
    private static final long serialVersionUID = 1L;
    /**
     * 订单号
     */
    private String orderNo;
    /**
     * 自定义的唯一任务名
     */
    private String jobName;

    private String applicationId;

    private String jobId;

}