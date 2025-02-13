package io.github.collin.cdc.mysql.cdc.iceberg.function;

import cn.hutool.core.io.FileUtil;
import io.github.collin.cdc.common.enums.OpType;
import io.github.collin.cdc.common.util.JacksonUtil;
import io.github.collin.cdc.mysql.cdc.common.dto.RowJson;
import io.github.collin.cdc.mysql.cdc.iceberg.adapter.RobotAdapter;
import io.github.collin.cdc.mysql.cdc.iceberg.dto.cache.PropertiesCacheDTO;
import lombok.RequiredArgsConstructor;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;

import java.io.File;

/**
 * 物理删除过滤
 *
 * @author collin
 * @date 2023-05-26
 */
@RequiredArgsConstructor
public class DeletedFilterFunction extends RichFilterFunction<RowJson> {

    /**
     * 自定义任务名
     */
    private final String application;
    /**
     * properties缓存文件名
     */
    private final String propertiesCacheFileName;
    private transient RobotAdapter robotAdapter;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // 读取properties
        File propertiesFile = getRuntimeContext().getDistributedCache().getFile(propertiesCacheFileName);
        String propertiesJson = FileUtil.readUtf8String(propertiesFile);
        PropertiesCacheDTO propertiesCache = JacksonUtil.parseObject(propertiesJson, PropertiesCacheDTO.class);
        this.robotAdapter = new RobotAdapter(propertiesCache.getProxy(), propertiesCache.getMonitor());
    }

    @Override
    public boolean filter(RowJson value) throws Exception {
        if (value.getOp() != OpType.DELETE) {
            return true;
        }

        robotAdapter.noticeAfterReceiveDelete(application, value.getDb(), value.getTable(), JacksonUtil.toJson(value.getJson()));
        // 物理删除，不处理
        return false;
    }

}