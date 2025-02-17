package io.github.collin.cdc.mysql.cdc.ods.test;

import io.github.collin.cdc.common.enums.YamlEnv;
import io.github.collin.cdc.mysql.cdc.ods.util.CdcUtil;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class MysqlCdcTest {

    @Test
    public void testSyncIceberg() throws Exception {
        CdcUtil.createMySQLSyncDatabase(new String[]{"iceberg/application-biz-dev.yaml"});
    }

    @Test
    public void testSyncDevStarRocks() throws Exception {
        CdcUtil.createMySQLSyncDatabase(new String[]{"starrocks/dev/application-biz.yaml"});
    }

    @Test
    public void testFixData() throws Exception {
        CdcUtil.createMySQLSyncDatabase(new String[]{"starrocks/dev/fix/application-business.yaml"});
    }

}