package io.github.collin.cdc.ods;

import io.github.collin.cdc.common.enums.YamlEnv;
import io.github.collin.cdc.ods.cdc.Mysql2IcebergOdsHandler;

/**
 * 测试环境启动类
 *
 * @author collin
 * @date 2023-05-06
 */
public class AppTest {

    public static void main(String[] args) throws Exception {
        new Mysql2IcebergOdsHandler(YamlEnv.TEST).run();
    }

}