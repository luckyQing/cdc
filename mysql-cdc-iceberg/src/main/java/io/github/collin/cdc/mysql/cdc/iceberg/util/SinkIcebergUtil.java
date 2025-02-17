package io.github.collin.cdc.mysql.cdc.iceberg.util;

import io.github.collin.cdc.common.util.IcebergUtil;
import io.github.collin.cdc.mysql.cdc.common.constants.FieldConstants;
import io.github.collin.cdc.mysql.cdc.common.dto.ColumnMetaDataDTO;
import io.github.collin.cdc.mysql.cdc.iceberg.dto.PartitionFieldDTO;
import io.github.collin.cdc.mysql.cdc.iceberg.enums.MysqlType2IcebergMapping;
import io.github.collin.cdc.mysql.cdc.iceberg.enums.PartitionType;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class SinkIcebergUtil {

    /**
     * 获取表字段
     *
     * @param columnMetaDatas
     * @param isSharding
     * @return
     */
    public static List<Types.NestedField> getColumns(List<ColumnMetaDataDTO> columnMetaDatas, Set<String> partitionFields, boolean isSharding) {
        List<Types.NestedField> nestedFields = new ArrayList<>(columnMetaDatas.size());
        if (isSharding) {
            Types.NestedField dbNestedField = Types.NestedField.required(1, FieldConstants.DB_NAME, Types.StringType.get(), FieldConstants.COMMENT_DB_NAME);
            nestedFields.add(dbNestedField);

            Types.NestedField tableNestedField = Types.NestedField.required(2, FieldConstants.TABLE_NAME, Types.StringType.get(), FieldConstants.COMMENT_TABLE_NAME);
            nestedFields.add(tableNestedField);
        }

        for (int i = 0; i < columnMetaDatas.size(); i++) {
            ColumnMetaDataDTO columnMetaData = columnMetaDatas.get(i);
            Types.NestedField nestedField = null;
            if (columnMetaData.isPrimaryKey() || (partitionFields != null && partitionFields.contains(columnMetaData.getName()))) {
                nestedField = Types.NestedField.required((nestedFields.size() + 1), columnMetaData.getName(), MysqlType2IcebergMapping.of(columnMetaData.getMysqlType()), columnMetaData.getComment());
            } else {
                nestedField = Types.NestedField.optional((nestedFields.size() + 1), columnMetaData.getName(), MysqlType2IcebergMapping.of(columnMetaData.getMysqlType()), columnMetaData.getComment());
            }
            nestedFields.add(nestedField);
        }

        IcebergUtil.addSyncTsColumn(nestedFields);
        return nestedFields;
    }

    /**
     * 获取表主键字段名
     *
     * @param columnMetaDatas
     * @return
     */
    public static Set<Integer> getPrimaryKeyNames(List<ColumnMetaDataDTO> columnMetaDatas, Set<String> partitionFields, boolean isSharding) {
        Set<Integer> primaryKeyNames = null;
        int startIndex = 0;
        if (isSharding) {
            primaryKeyNames = new LinkedHashSet<>();
            primaryKeyNames.add(1);
            primaryKeyNames.add(2);
            startIndex = 2;
        }

        for (int i = 0; i < columnMetaDatas.size(); i++) {
            ColumnMetaDataDTO columnMetaData = columnMetaDatas.get(i);
            if (columnMetaData.isPrimaryKey() || (partitionFields != null && partitionFields.contains(columnMetaData.getName()))) {
                if (primaryKeyNames == null) {
                    primaryKeyNames = new LinkedHashSet<>();
                }
                primaryKeyNames.add(startIndex + i + 1);
            }
        }

        return primaryKeyNames;
    }

    public static PartitionSpec buildPartitionSpec(Schema schema, String targetDbNameAndTableName) {
        List<PartitionFieldDTO> partitionFields = PartitionUtil.getPartitionFields(targetDbNameAndTableName);
        if (partitionFields != null) {
            PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);
            for (PartitionFieldDTO partitionField : partitionFields) {
                if (partitionField.getPartitionType() == PartitionType.HASH) {
                    builder.bucket(partitionField.getName(), partitionField.getNumBuckets());
                } else if (partitionField.getPartitionType() == PartitionType.DAY) {
                    builder.day(partitionField.getName());
                } else if (partitionField.getPartitionType() == PartitionType.MONTH) {
                    builder.month(partitionField.getName());
                } else if (partitionField.getPartitionType() == PartitionType.YEAR) {
                    builder.year(partitionField.getName());
                }
            }

            return builder.build();
        } else {
            return PartitionSpec.unpartitioned();
        }
    }

}