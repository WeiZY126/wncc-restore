package config;

import com.alibaba.fastjson.JSONObject;
import custom.CustomFlatMapFunction;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.OutputTag;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import utils.ParseJSON2MapUtils;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.iceberg.flink.FlinkCatalogFactory.*;

/**
 * Author: EtherealQ (XQ)
 * Date: 2023/3/8
 * Description: iceberg配置初始化
 */

@Slf4j
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class IcebergConfig implements Serializable {
    public Tuple2<TableLoader, JsonRowDataDeserializationSchema> tuple2;
    public List<String> equalityFieldColumns;
    private String path;

    /**
     * Description: 初始化配置文件
     */
    public IcebergConfig(String path) {
        this.path = path;
        File file = new File(path);
        String str = null;
        try {
            str = FileUtils.readFileToString(file, "UTF-8");
        } catch (IOException e) {
            //log.error("Error reading" + path + "file");
        }
        Map<String, Object> mapForJson = ParseJSON2MapUtils.parseJSON2Map(str);
        this.tuple2 = initTableLoader((JSONObject) mapForJson.get("iceberg"));
    }

    private Tuple2<TableLoader, JsonRowDataDeserializationSchema> initTableLoader(JSONObject js) {
        Map<String, String> prop = new HashMap<>();
        prop.put(TYPE, "iceberg");
        prop.put(PROPERTY_VERSION, "1");
        prop.put("warehouse", js.getString("warehousePath"));
        CatalogLoader catalogLoader;
        if (js.getString("catalogType").equals(ICEBERG_CATALOG_TYPE_HADOOP)) {
            prop.put(ICEBERG_CATALOG_TYPE, ICEBERG_CATALOG_TYPE_HADOOP);
            catalogLoader = CatalogLoader.hadoop(js.getString("catalogName"), new org.apache.hadoop.conf.Configuration(), prop);
        } else {
            prop.put(ICEBERG_CATALOG_TYPE, ICEBERG_CATALOG_TYPE_HIVE);
            prop.put("uri", js.getString("hiveCatalogUri"));
            catalogLoader = CatalogLoader.hive(js.getString("catalogName"), new org.apache.hadoop.conf.Configuration(), prop);
        }
        final Catalog catalog = catalogLoader.loadCatalog();
        final TableIdentifier tableIdentifier = TableIdentifier.of(js.getString("database"), js.getString("table"));
        final Table table = catalog.loadTable(tableIdentifier);
        this.equalityFieldColumns = getEqualityFieldColumns(table);
        final Schema schema = table.schema();
        // json-RowData 序列化
        final RowType rowType = FlinkSchemaUtil.convert(schema);
        final JsonRowDataDeserializationSchema jsonRowDataDeserializationSchema =
                new JsonRowDataDeserializationSchema(rowType, InternalTypeInfo.of(rowType),
                        false, false, TimestampFormat.SQL);
        return Tuple2.of(TableLoader.fromCatalog(catalogLoader, tableIdentifier), jsonRowDataDeserializationSchema);
    }

    /**
     * 构建 EqualityFieldColumn
     *
     * @param table iceberg表
     * @return List
     */
    public static List<String> getEqualityFieldColumns(Table table) {
        List<String> equalityFieldColumns = new ArrayList<>();
        Schema schema = table.schema();
        String priKey = "";
        List<Types.NestedField> columns = schema.columns();
        for (Types.NestedField a : columns) {
            if (a.isRequired()) {
                priKey = a.name();
                equalityFieldColumns.add(a.name());
            }
        }
        List<PartitionField> fields = table.spec().fields();
        for (PartitionField partitionField : fields) {
            if (!(partitionField.name().startsWith("shard") || partitionField.name().startsWith(priKey)))
                equalityFieldColumns.add(partitionField.name());
        }
        return equalityFieldColumns;
    }

    public SingleOutputStreamOperator<RowData> setSinkTo(DataStream<String> stream, int writeParallelism, OutputTag<String> errorTag) {
        //数据提取
        //TODO by weizy 新传入tableLoader，用于异常处理
        SingleOutputStreamOperator<RowData> sinkIceberg = stream.process(new CustomFlatMapFunction(this.tuple2.f1,errorTag,this.tuple2.f0), this.tuple2.f1.getProducedType());
//        sinkIceberg.print();

        FlinkSink.forRowData(sinkIceberg).writeParallelism(writeParallelism)
                .upsert(true)
                .distributionMode(DistributionMode.HASH)
                .tableLoader(this.tuple2.f0)
                .equalityFieldColumns(this.equalityFieldColumns)
                .append();
        return sinkIceberg;
    }
}