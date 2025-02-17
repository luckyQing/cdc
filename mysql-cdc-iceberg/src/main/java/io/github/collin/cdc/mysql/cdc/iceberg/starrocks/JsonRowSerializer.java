package io.github.collin.cdc.mysql.cdc.iceberg.starrocks;

import com.starrocks.connector.flink.cdc.StarRocksOptions;
import io.github.collin.cdc.common.enums.OpType;
import io.github.collin.cdc.common.util.JacksonUtil;
import io.github.collin.cdc.mysql.cdc.common.dto.RowJson;
import io.github.collin.cdc.mysql.cdc.iceberg.util.SinkStarRocksUtil;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Serialize {@link RowJson} to json string.
 *
 * @author collin
 * @date 2024-12-02
 */
@Slf4j
public class JsonRowSerializer implements Serializable {

    public static final String INVALID_RESULT = "invalid result";
    private static final String STARROCKS_DELETE_SIGN = "__op";

    private final String database;
    private final String table;
    private final EnhanceStarRocksCatalog starRocksCatalog;
    private final Boolean isFastSchemaEvolution;

    public JsonRowSerializer(StarRocksOptions starRocksOptions) {
        String[] tableInfo = starRocksOptions.getTableIdentifier().split("\\.");
        this.database = tableInfo[0];
        this.table = tableInfo[1];
        this.starRocksCatalog = new EnhanceStarRocksCatalog(starRocksOptions.getOpts().getDbURL(), starRocksOptions.getOpts().getUsername().get(), starRocksOptions.getOpts().getPassword().get());
        this.isFastSchemaEvolution = starRocksOptions.getFastSchemaEvolution();
        this.starRocksCatalog.open();
    }

    public String process(RowJson value) throws IOException {
        if (value.getOp() == OpType.DDL) {
            // schema change ddl
            if (isFastSchemaEvolution) {
                extractDDLAndExecute(value.getDdl());
            }
            return INVALID_RESULT;
        }

        Map<String, Object> valueMap = value.getJson();
        if (value.getOp() == OpType.INSERT || value.getOp() == OpType.UPDATE_AFTER) {
            addDeleteSign(valueMap, false);
        } else if (value.getOp() == OpType.DELETE) {
            addDeleteSign(valueMap, true);
        } else {
            log.error("parse record fail, unknown op| {}", JacksonUtil.toJson(value));
            return INVALID_RESULT;
        }

        return JacksonUtil.toJson(valueMap);
    }

    private void addDeleteSign(Map<String, Object> valueMap, boolean delete) {
        if (delete) {
            valueMap.put(STARROCKS_DELETE_SIGN, "1");
        } else {
            valueMap.put(STARROCKS_DELETE_SIGN, "0");
        }
    }

    private void extractDDLAndExecute(String ddl) {
        try {
            if (ddl != null) {
                log.warn("ddl==>{}", ddl);
                long timeoutSecond = 30;
                List<String> sqls = SinkStarRocksUtil.convertStarRocksSql(ddl, database, table, timeoutSecond);
                if (sqls.isEmpty()) {
                    return;
                }

                sqls.forEach(sql -> {
                    starRocksCatalog.executeAlter(database, table, sql, timeoutSecond);
                    log.warn("sync ddl success==>{}", sql);
                });
            }
        } catch (Exception e) {
            log.error("sync ddl fail|{}", ddl, e);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for JsonDebeziumSchemaSerializer.
     */
    public static class Builder {
        private StarRocksOptions starRocksOptions;

        public Builder setStarRocksOptions(StarRocksOptions starRocksOptions) {
            this.starRocksOptions = starRocksOptions;
            return this;
        }

        public JsonRowSerializer build() {
            return new JsonRowSerializer(starRocksOptions);
        }
    }

}