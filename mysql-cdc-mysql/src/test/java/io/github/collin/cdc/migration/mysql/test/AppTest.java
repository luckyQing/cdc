package io.github.collin.cdc.migration.mysql.test;

import io.github.collin.cdc.common.enums.YamlEnv;
import io.github.collin.cdc.migration.mysql.cdc.MigrationHandler;
import lombok.extern.slf4j.Slf4j;
import org.junit.Ignore;
import org.junit.Test;

@Slf4j
@Ignore
public class AppTest {

    @Test
    public void test() throws Exception {
        new MigrationHandler(YamlEnv.DEFAULT.getFileName()).execute();
    }

}