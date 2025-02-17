package io.github.collin.cdc.mysql.cdc.iceberg.util;

import cn.hutool.core.date.DateUtil;
import com.google.common.collect.Lists;
import io.github.collin.cdc.mysql.cdc.iceberg.dto.PartitionFieldDTO;
import io.github.collin.cdc.mysql.cdc.iceberg.enums.PartitionType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 分区工具类
 *
 * @author collin
 * @date 2023-11-03
 */
public class PartitionUtil {

    /**
     * 分区字段信息（<目标表库名.表名, 分区字段信息>）
     */
    public static final Map<String, List<PartitionFieldDTO>> PARTITION_INFOS = new HashMap<>();

    static {
        String t_anti_fraud_desicion_yyyy_db_t_scene_desicion_mmdd = "rc_indonesia_db.t_anti_fraud_desicion_yyyy_db_t_scene_desicion_mmdd";
        PARTITION_INFOS.put(t_anti_fraud_desicion_yyyy_db_t_scene_desicion_mmdd,
                Lists.newArrayList(new PartitionFieldDTO().setName("Forder_id").setNumBuckets(10).setPartitionType(PartitionType.HASH),
                        new PartitionFieldDTO().setName("Fcreate_time").setPartitionType(PartitionType.MONTH).setStartTableTime(DateUtil.parseDateTime("2019-01-01 00:00:00"))));

        String t_strategy_decision_record_xx_db_t_rc_decision_record_y = "rc_indonesia_db.t_strategy_decision_record_xx_db_t_rc_decision_record_y";
        PARTITION_INFOS.put(t_strategy_decision_record_xx_db_t_rc_decision_record_y,
                Lists.newArrayList(new PartitionFieldDTO().setName("Forder_id").setNumBuckets(10).setPartitionType(PartitionType.HASH),
                        new PartitionFieldDTO().setName("Fcreate_time").setPartitionType(PartitionType.MONTH).setStartTableTime(DateUtil.parseDateTime("2019-01-01 00:00:00"))));

        String t_vendor_rlog_yyyyMM = "lepin_log_ext.t_vendor_rlog_yyyyMM";
        PARTITION_INFOS.put(t_vendor_rlog_yyyyMM,
                Lists.newArrayList(new PartitionFieldDTO().setName("create_time").setPartitionType(PartitionType.MONTH).setStartTableTime(DateUtil.parseDateTime("2019-01-01 00:00:00"))));
    }

    public static Set<String> getPartitionFieldSet(String targetDbNameAndTableName) {
        List<PartitionFieldDTO> partitionFields = PARTITION_INFOS.get(targetDbNameAndTableName);
        if (partitionFields == null) {
            return null;
        }
        return partitionFields.stream().map(PartitionFieldDTO::getName).collect(Collectors.toSet());
    }

    public static List<PartitionFieldDTO> getPartitionFields(String targetDbNameAndTableName) {
        return PARTITION_INFOS.get(targetDbNameAndTableName);
    }

}