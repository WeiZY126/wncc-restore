package com.sitech.test;

import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.NestedRowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Iterators;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.Deletes;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.io.File;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedAction;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class test {
    public static void main(String[] args) throws Exception {

        HadoopCatalog hadoopCatalog = new HadoopCatalog();
        Configuration configuration = new Configuration();
        configuration.addResource(new File("C:\\Users\\wzy\\Desktop\\数据集成平台\\wncc-restore\\restore\\ExceptionHandler\\src\\main\\resources\\core-site.xml").toURI().toURL());
        configuration.addResource(new File("C:\\Users\\wzy\\Desktop\\数据集成平台\\wncc-restore\\restore\\ExceptionHandler\\src\\main\\resources\\hdfs-site.xml").toURI().toURL());
        hadoopCatalog.setConf(configuration);
        UserGroupInformation.setConfiguration(configuration);
        UserGroupInformation userGroupInformation = UserGroupInformation.loginUserFromKeytabAndReturnUGI("e3base/e3base01", "C:\\Users\\wzy\\Desktop\\e3base.keytab");
        userGroupInformation.doAs(new PrivilegedAction<Object>() {
            @SneakyThrows
            @Override
            public Object run() {
                TableLoader tableLoader = TableLoader.fromHadoopTable("hdfs://drmcluster/iceberg/hadoop/warehouse/restore_catalog/testdb/PERSON", configuration);
                tableLoader.open();
                Table table = tableLoader.loadTable();
                Schema schema = table.schema();
                Map<String, Type.TypeID> columnMap = new LinkedHashMap<>();
                for (Types.NestedField column : schema.columns()) {
                    columnMap.put(column.name(), column.type().typeId());
                }
//                CloseableIterable<Record> cust_id = IcebergGenerics.read(table)
//                        .where(Expressions.equal("CUST_ID", Long.valueOf(1)))
//                        .select("*")
//                        .build();
                IcebergGenerics.ScanBuilder read = IcebergGenerics.read(table);
                JSONObject object = JSON.parseObject("{\"NAME\":\"AK11\"}");
                Integer size = object.size();
                Expression[] expressions = new Expression[size];
                AtomicReference<Integer> pos = new AtomicReference<>(0);
                object.forEach((key, value) -> {
                    Type.TypeID typeID = columnMap.get(key);
                    value = caseValueToJavaType(typeID, value);
                    //根据能识别出来的value进行筛选
                    if (value != null) {
                        expressions[pos.getAndSet(pos.get() + 1)] = Expressions.equal(key, value);
                    }
                });
                Expression[] finalExpressions = Arrays.copyOfRange(expressions, 0, pos.get());
                CloseableIterable<Record> build = read.where(Expressions.and(Expressions.alwaysTrue(), Expressions.alwaysTrue(), finalExpressions))
                        .select("*")
                        .build();
                System.out.println(Iterators.size(build.iterator()));
                Record record = build.iterator().next();
                columnMap.forEach((k, v) -> {
                    System.out.println(k + "--" + v);
                });
                for (int i = 0; i < columnMap.size(); i++) {
                    System.out.println(record.get(i) + "----" + record.get(i).getClass());
                }

                Transaction transaction = table.newTransaction();
//                CloseableIterable<FileScanTask> fileScanTasks = table
//                        .newScan()
//                        .filter(Expressions.and(Expressions.alwaysTrue(), Expressions.alwaysTrue(), finalExpressions))
//                        .planFiles();

                RowDelta rowDelta = transaction.newRowDelta();
                transaction
                        .newDelete()
                        .deleteFromRowFilter(Expressions.and(Expressions.alwaysTrue(), Expressions.alwaysTrue(), finalExpressions))
                        .commit();

                // json-RowData 序列化
//                final RowType rowType = FlinkSchemaUtil.convert(schema);
//                final JsonRowDataDeserializationSchema jsonRowDataDeserializationSchema =
//                        new JsonRowDataDeserializationSchema(rowType, InternalTypeInfo.of(rowType),
//                                false, false, TimestampFormat.SQL);
//                RowData rowData = jsonRowDataDeserializationSchema.deserialize(object.toJSONString().getBytes(StandardCharsets.UTF_8));
//                System.out.println(rowData.isNullAt(0));
//                System.out.println(rowData.getString(1));
                GenericRowData afterRowData = new GenericRowData(columnMap.size());
                int pos2 = 0;
                for (Map.Entry<String, Type.TypeID> entry : columnMap.entrySet()) {
                    String column = entry.getKey();
                    Type.TypeID type = entry.getValue();
                    String jsonValue = object.getString(column);
                    if (object.getString(column) == null) {
                        assembleJavaTypeToRowData(record.getField(column), afterRowData, pos2);
                    } else {
                        assembleJavaTypeToRowData(caseValueToJavaType(type, jsonValue), afterRowData, pos2);
                    }
                    pos2++;
                }
                GenericRecord genericRecord = GenericRecord.create(schema);
                for (Map.Entry<String, Type.TypeID> entry : columnMap.entrySet()) {
                    String column = entry.getKey();
                    Type.TypeID type = entry.getValue();
                    String jsonValue = object.getString(column);
                    if (object.getString(column) == null) {
                        genericRecord.setField(column, record.getField(column));
//                        assembleJavaTypeToRowData(record.getField(column), afterRowData, pos2);
                    } else {
                        genericRecord.setField(column, caseValueToJavaType(type, jsonValue));
//                        assembleJavaTypeToRowData(caseValueToJavaType(type, jsonValue), afterRowData, pos2);
                    }
                    pos2++;
                }
                genericRecord.setField("NAME", "ww");
                DataWriter<GenericRecord> dataWriter = Parquet.writeData(table.io().newOutputFile(table.location() + "/" + UUID.randomUUID().toString()))
                        .schema(schema)
                        .createWriterFunc(GenericParquetWriter::buildWriter)
                        .overwrite(true)
                        .withSpec(PartitionSpec.unpartitioned())
                        .build();
                dataWriter.write(genericRecord);
                dataWriter.close();
                DataFile dataFile = dataWriter.toDataFile();
                transaction.newAppend().appendFile(dataFile).commit();

                transaction.commitTransaction();
                System.out.println(afterRowData.toString());

                return null;
            }
        });

    }

    private static Object caseValueToJavaType(Type.TypeID typeID, Object value) {
        switch (typeID) {
            case BOOLEAN:
                return Boolean.valueOf((String) value);
            case INTEGER:
                return Integer.valueOf((String) value);
            case LONG:
                return Long.valueOf((String) value);
            case TIMESTAMP:
//                CharSequence charSequence = (String) value;
//                return java.time.OffsetDateTime.parse(charSequence).toString();
            case TIME:
            case DATE:
//                CharSequence charSequence = (String) value;
//                return java.time.LocalDate.parse(charSequence);
            case STRING:
                return (CharSequence) value;
            case FLOAT:
                return Float.valueOf((String) value);
            case DOUBLE:
                return Double.valueOf((String) value);
            case UUID:
                return java.util.UUID.fromString((String) value);
            case FIXED:
            case BINARY:
                return ByteBuffer.wrap(((String) value).getBytes(StandardCharsets.UTF_8));
            case DECIMAL:
                return BigDecimal.valueOf(Long.valueOf((String) value));
            case STRUCT:
            case LIST:
            case MAP:
            default:
                return null;
        }
    }

    private static void assembleJavaTypeToRowData(Object value, GenericRowData rowData, Integer pos) {
        if (value instanceof Boolean) {
            rowData.setField(pos, value);
        } else if (value instanceof Integer) {
            rowData.setField(pos, value);
        } else if (value instanceof Long) {
            rowData.setField(pos, value);
        } else if (value instanceof Float) {
            rowData.setField(pos, value);
        } else if (value instanceof Double) {
            rowData.setField(pos, value);
        } else if (value instanceof String) {
            rowData.setField(pos, StringData.fromString((String) value));
        } else if (value instanceof java.time.LocalDate) {
            rowData.setField(pos, StringData.fromString(value.toString()));
        } else if (value instanceof java.time.OffsetDateTime) {
            rowData.setField(pos, StringData.fromString(value.toString()));
        } else if (value instanceof UUID) {
            rowData.setField(pos, value);
        } else if (value instanceof ByteBuffer) {
            rowData.setField(pos, value);
        } else if (value instanceof BigDecimal) {
            rowData.setField(pos, DecimalData.fromBigDecimal((BigDecimal) value, ((BigDecimal) value).precision(), ((BigDecimal) value).scale()));
        } else {
            rowData.setField(pos, value);
        }
    }

    private CloseableIterable<Record> getRecord(Table table, JSONObject object) {
        IcebergGenerics.ScanBuilder read = IcebergGenerics.read(table);
        object.forEach((key, value) -> {
            read.where(Expressions.equal(key, value));
        });
        return read.select().build();
    }

    private void getRowData(CloseableIterable<Record> recordCloseableIterable, List<Types.NestedField> columns) {
        Record record = recordCloseableIterable.iterator().next();
        GenericRowData rowData = new GenericRowData(columns.size());
        int pos = 0;
        for (Types.NestedField column : columns) {
            Type.TypeID typeID = column.type().typeId();
            record.getField(column.name());
//            switch (typeID) {
//                case Type.TypeID.BOOLEAN:
//                    rowData.setField(pos, Boolean.valueOf());
//                    break;
//            }
        }
    }
}
