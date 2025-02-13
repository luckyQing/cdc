package io.github.collin.cdc.mysql.cdc.iceberg.test;

import io.github.collin.cdc.common.enums.YamlEnv;
import io.github.collin.cdc.mysql.cdc.iceberg.cdc.Mysql2IcebergOdsHandler;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class AppTest {

    @Test
    public void testRun() throws Exception {
        new Mysql2IcebergOdsHandler(YamlEnv.DEFAULT.getFileName()).run();
    }

}