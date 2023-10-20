package io.github.collin.cdc.common.enums;

import lombok.AllArgsConstructor;

/**
 * namespace环境值定义
 *
 * @author collin
 * @date 2023-08-25
 */
@AllArgsConstructor
public enum Namespaces {

    /**
     * ods库前缀
     */
    ODS_PRE("test_ods_", "ods_"),
    /**
     * dwd层库名
     */
    DWD("test_dwd", "dwd");

    /**
     * 测试环境值
     */
    private String test;
    /**
     * 生产环境值
     */
    private String prod;

    /**
     * 获取ods namespace前缀
     *
     * @param env
     * @return
     * @see Env
     */
    public static String getOdsPre(String env) {
        return getValue(Env.valueOf(env), Namespaces.ODS_PRE);
    }

    /**
     * 获取dwd库名
     *
     * @param env
     * @return
     * @see Env
     */
    public static String getDwd(String env) {
        return getValue(Env.valueOf(env), Namespaces.DWD);
    }

    private static String getValue(Env env, Namespaces namespaces) {
        if (env == Env.PROD) {
            return namespaces.prod;
        }

        return namespaces.test;
    }
}