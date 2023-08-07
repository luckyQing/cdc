package io.github.collin.cdc.common.util;

import io.github.collin.cdc.common.constants.SchemaConstants;
import io.github.collin.cdc.common.properties.HdfsProperties;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkCatalogFactory;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * iceberg工具类
 *
 * @author collin
 * @date 2023-05-31
 */
public class IcebergUtil {

    /**
     * 添加同步字段
     *
     * @param columns
     */
    public static void addSyncTsColumn(List<Types.NestedField> columns) {
        columns.add(Types.NestedField.optional(columns.size() + 1, SchemaConstants.SYNC_TS, Types.LongType.get(), "同步时间"));
    }

    public static Table getExistTable(HdfsProperties hdfsProperties, String dbName, String tableName) {
        CatalogLoader catalogLoader = IcebergUtil.catalogConfiguration(hdfsProperties);
        Catalog catalog = catalogLoader.loadCatalog();
        TableIdentifier identifier = TableIdentifier.of(Namespace.of(dbName), tableName);
        if (catalog.tableExists(identifier)) {
            return catalog.loadTable(identifier);
        }

        return null;
    }

    /**
     * 创建表
     *
     * @param schema
     * @param catalog
     * @param identifier
     * @return
     */
    public static Table createTable(Schema schema, Catalog catalog, TableIdentifier identifier) {
        return createTable(schema, catalog, identifier, true);
    }

    /**
     * 创建表
     *
     * @param schema
     * @param catalog
     * @param identifier
     * @param enableUpsert
     * @return
     */
    public static Table createTable(Schema schema, Catalog catalog, TableIdentifier identifier, boolean enableUpsert) {
        return createTable(schema, catalog, identifier, enableUpsert, null);
    }

    /**
     * 创建表
     *
     * @param schema
     * @param catalog
     * @param identifier
     * @param enableUpsert
     * @param writeTargetFileSizeBytes
     * @return
     */
    public static Table createTable(Schema schema, Catalog catalog, TableIdentifier identifier, boolean enableUpsert, Long writeTargetFileSizeBytes) {
        if (catalog.tableExists(identifier)) {
            return catalog.loadTable(identifier);
        }

        Map<String, String> prop = new HashMap<>(8);
        prop.put(TableProperties.FORMAT_VERSION, "2");
        prop.put("iceberg.mr.catalog", "hive");
        prop.put(TableProperties.ENGINE_HIVE_ENABLED, "true");
        prop.put(TableProperties.UPSERT_ENABLED, String.valueOf(enableUpsert));
        if (writeTargetFileSizeBytes != null) {
            prop.put(TableProperties.WRITE_TARGET_FILE_SIZE_BYTES, String.valueOf(writeTargetFileSizeBytes));
        }
        prop.put("hive.vectorized.execution.enabled", "false");
        // 删除老的metadata files
        prop.put(TableProperties.METADATA_PREVIOUS_VERSIONS_MAX, "3");
        prop.put(TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED, "true");
        if (schema.findField(SchemaConstants.SYNC_TS) != null) {
            prop.put("write.parquet.bloom-filter-enabled.column.f_ts", "true");
        }
        // 空文件提交触发数量（IcebergFilesCommitter#MAX_CONTINUOUS_EMPTY_COMMITS）
        prop.put("flink.max-continuous-empty-commits", "100");
        prop.put(TableProperties.MANIFEST_MIN_MERGE_COUNT, "30");
        // 快照保存2小时
        prop.put(TableProperties.MAX_SNAPSHOT_AGE_MS, "7200000");
        Catalog.TableBuilder tableBuilder = catalog.buildTable(identifier, schema).withPartitionSpec(PartitionSpec.unpartitioned()).withProperties(prop);

        // trino查询时的结果如果有多条，则会随机返回；最好设置排序
        // 根据主键排序
        Set<String> identifierFieldNames = schema.identifierFieldNames();
        if (identifierFieldNames != null && !identifierFieldNames.isEmpty()) {
            SortOrder.Builder builder = SortOrder.builderFor(schema);
            for (String identifierFieldName : identifierFieldNames) {
                builder.asc(identifierFieldName);
            }

            tableBuilder.withSortOrder(builder.build());
        }
        Table table = tableBuilder.create();
        // need to upgrade version to 2,otherwise 'java.lang.IllegalArgumentException: Cannot write delete files in a v1 table'
        TableOperations operations = ((BaseTable) table).operations();
        TableMetadata metadata = operations.current();
        operations.commit(metadata, metadata.upgradeToFormatVersion(2));
        return table;
    }

    public static CatalogLoader catalogConfiguration(HdfsProperties hdfsProperties) {
        Map<String, String> properties = new HashMap<>(8);
        properties.put(FlinkCatalogFactory.TYPE, "iceberg");
        properties.put(FlinkCatalogFactory.ICEBERG_CATALOG_TYPE, FlinkCatalogFactory.ICEBERG_CATALOG_TYPE_HIVE);
        properties.put(FlinkCatalogFactory.PROPERTY_VERSION, "1");
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, hdfsProperties.getWarehouse());
        properties.put(CatalogProperties.URI, hdfsProperties.getUri());

        return CatalogLoader.hive(hdfsProperties.buildCatalog(), new Configuration(), properties);
    }

    /**
     * 行级删除
     *
     * @param tableResult        待删除的数据
     * @param table              待删除数据所在表
     * @param equalityFieldNames 主键字段名
     * @throws IOException
     * @since https://github.com/apache/iceberg/issues/1724
     */
    public static void deleteRows(TableResult tableResult, Table table, String... equalityFieldNames) throws IOException {
        try (CloseableIterator<Row> rowIterator = tableResult.collect()) {
            if (!rowIterator.hasNext()) {
                return;
            }

            OutputFileFactory fileFactory = OutputFileFactory.builderFor(table, 1, 1).build();
            OutputFile outputFile = fileFactory.newOutputFile().encryptingOutputFile();

            Schema deleteRowSchema = table.schema().select(equalityFieldNames);
            Record deleteRecord = GenericRecord.create(deleteRowSchema);

            EqualityDeleteWriter<Record> writer = null;
            try {
                writer = Parquet.writeDeletes(outputFile).forTable(table).withPartition(null).rowSchema(deleteRowSchema).createWriterFunc(GenericParquetWriter::buildWriter).overwrite().equalityFieldIds(deleteRowSchema.columns().stream().mapToInt(Types.NestedField::fieldId).toArray()).buildEqualityWriter();

                int fieldSize = equalityFieldNames.length;
                while (rowIterator.hasNext()) {
                    Row row = rowIterator.next();

                    Map<String, Object> overwriteValues = Maps.newHashMapWithExpectedSize(fieldSize);
                    for (String equalityFieldName : equalityFieldNames) {
                        overwriteValues.put(equalityFieldName, row.getField(equalityFieldName));
                    }
                    writer.write(deleteRecord.copy(overwriteValues));
                }
            } finally {
                if (writer != null) {
                    writer.close();
                }
            }

            DeleteFile eqDeletes = writer.toDeleteFile();

            table.newRowDelta().addDeletes(eqDeletes).commit();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}