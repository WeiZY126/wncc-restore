package test;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
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
import org.apache.iceberg.io.CloseableIterable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.apache.iceberg.flink.FlinkCatalogFactory.ICEBERG_CATALOG_TYPE;
import static org.apache.iceberg.flink.FlinkCatalogFactory.ICEBERG_CATALOG_TYPE_HADOOP;
import static org.apache.iceberg.flink.FlinkCatalogFactory.ICEBERG_CATALOG_TYPE_HIVE;
import static org.apache.iceberg.flink.FlinkCatalogFactory.PROPERTY_VERSION;
import static org.apache.iceberg.flink.FlinkCatalogFactory.TYPE;

@Slf4j
public class KafkaReaderRowDataTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KafkaSourceBuilder<String> builder = KafkaSource.builder();
        KafkaSource<String> kafkaSource = builder
                .setBootstrapServers("172.21.9.101:9092,172.21.9.102:9092,172.21.9.103:9092")
                .setGroupId("weizy_test")
                .setTopics("weizy_test")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(OffsetsInitializer.latest())
                .build();
        DataStreamSource<String> source = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka-Source");
        source.process(new testProcess());
    }

    static class testProcess extends ProcessFunction<String, RowData> {

        private final JsonRowDataDeserializationSchema jrd;
        private final TableLoader tableLoader;

        public testProcess() {
            this.tableLoader = initTableLoader().f0;
            tableLoader.open();
            Schema schema = tableLoader.loadTable().schema();
            final RowType rowType = FlinkSchemaUtil.convert(schema);
            final JsonRowDataDeserializationSchema jsonRowDataDeserializationSchema =
                    new JsonRowDataDeserializationSchema(rowType, InternalTypeInfo.of(rowType),
                            false, false, TimestampFormat.SQL);
            this.jrd = jsonRowDataDeserializationSchema;
        }

        @Override
        public void processElement(String str, Context ctx, Collector<RowData> out) throws Exception {
            if (str != null) {
                JSONObject jsonObject = JSON.parseObject(str);
                String op = jsonObject.getString("OP");
                String mode = null;
                try {
                    if (op.equals("I")) {
                        mode = "columnInfo";
                        RowData rowData = getRowData(jsonObject, mode);
                        rowData.setRowKind(RowKind.INSERT);
                        emit(ctx, out, jsonObject, rowData);
                    } else if (op.equals("U")) {
                        mode = "Before";
                        RowData rowData1 = getRowData(jsonObject, mode);
                        rowData1.setRowKind(RowKind.UPDATE_BEFORE);
                        emit(ctx, out, jsonObject, rowData1);
                        mode = "After";
                        RowData rowData2 = getRowData(jsonObject, mode);
                        rowData2.setRowKind(RowKind.UPDATE_AFTER);
                        emit(ctx, out, jsonObject, rowData2);
                    } else if (op.equals("D")) {
                        mode = "Before";
                        RowData rowData = getRowData(jsonObject, mode);
                        rowData.setRowKind(RowKind.DELETE);
                        emit(ctx, out, jsonObject, rowData);
                    }
                } catch (NullKeyException e) {
                    log.info("主键为空，开始尝试恢复");

                } catch (Exception e) {
                    //log.error(e.getMessage());
//                    ctx.output(errorTag, jsonObject.toString());
                }
            }
        }

        private RowData getRowData(JSONObject jsonObject, String mode) throws IOException {
            RowData rowData;
            rowData =
                    jrd.deserialize(jsonObject.getString(mode).getBytes(StandardCharsets.UTF_8));
            return rowData;
        }

        private void emit(ProcessFunction<String, RowData>.Context ctx, Collector<RowData> out, JSONObject jsonObject, RowData rowData) throws NullKeyException {
            if (!rowData.isNullAt(0)) {
                out.collect(rowData);
            } else {
                throw new NullKeyException();
                //log.error("关键信息为null");
//                ctx.output(errorTag, jsonObject.toString());
            }
        }

        private void handleNullKey(ProcessFunction<String, RowData>.Context ctx,
                                   Collector<RowData> out,
                                   JSONObject jsonObject,
                                   String mode,
                                   TableLoader tableLoader) {
            tableLoader.open();
            Table table = tableLoader.loadTable();
            JSONObject dataJsonObject = jsonObject.getJSONObject(mode);

//            CloseableIterable<Record> pk = IcebergGenerics.read(table)
//                    .where(Expressions.equal())
//                    .select("PK")
//                    .build();
//            pk.iterator().next().get(0);
        }

        private Tuple2<TableLoader, JsonRowDataDeserializationSchema> initTableLoader() {
            Map<String, String> prop = new HashMap<>();
            prop.put(TYPE, "iceberg");
            prop.put(PROPERTY_VERSION, "1");
            prop.put("warehouse", "");
            CatalogLoader catalogLoader;
//            if (js.getString("catalogType").equals(ICEBERG_CATALOG_TYPE_HADOOP)) {
//                prop.put(ICEBERG_CATALOG_TYPE, ICEBERG_CATALOG_TYPE_HADOOP);
//                catalogLoader = CatalogLoader.hadoop(js.getString("catalogName"), new org.apache.hadoop.conf.Configuration(), prop);
//            } else {
//                prop.put(ICEBERG_CATALOG_TYPE, ICEBERG_CATALOG_TYPE_HIVE);
//                prop.put("uri", js.getString("hiveCatalogUri"));
//                catalogLoader = CatalogLoader.hive(js.getString("catalogName"), new org.apache.hadoop.conf.Configuration(), prop);
//            }
            catalogLoader = CatalogLoader.hadoop("", new org.apache.hadoop.conf.Configuration(), prop);
            final Catalog catalog = catalogLoader.loadCatalog();
            final TableIdentifier tableIdentifier = TableIdentifier.of("", "");
            final Table table = catalog.loadTable(tableIdentifier);
            final Schema schema = table.schema();
            // json-RowData 序列化
            final RowType rowType = FlinkSchemaUtil.convert(schema);
            final JsonRowDataDeserializationSchema jsonRowDataDeserializationSchema =
                    new JsonRowDataDeserializationSchema(rowType, InternalTypeInfo.of(rowType),
                            false, false, TimestampFormat.SQL);
            return Tuple2.of(TableLoader.fromCatalog(catalogLoader, tableIdentifier), jsonRowDataDeserializationSchema);
        }
    }
}
