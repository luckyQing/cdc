package io.github.collin.cdc.common.enums;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public enum YamlEnv {

    /**
     * 本地配置
     */
    DEFAULT("application.yaml"),
    /**
     * 测试配置
     */
    TEST("application-test.yaml"),
    /**
     * 生产配置
     */
    PROD("application-prod.yaml"),
    /**
     * 业务生产配置
     */
    BIZ_PROD("application-biz-prod.yaml"),
    /**
     * 业务生产配置
     */
    REPAIR_PROD("application-repair-prod.yaml"),
    /**
     * 风控生产配置
     */
    RISK_PROD("application-risk-prod.yaml");

    private String fileName;

}