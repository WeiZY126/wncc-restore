package com.sitech.custom;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Iterators;
import com.sitech.utils.KryoUtils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionUtil;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class ExceptionHandlerProcess extends ProcessFunction<String, JSONObject> {
    private final OutputTag<String> errorTag;

    public ExceptionHandlerProcess(OutputTag<String> errorTag) {
        this.errorTag = errorTag;
    }

    @Override
    public void processElement(String s, Context ctx, Collector<JSONObject> collector) throws Exception {
        UserGroupInformation userGroupInformation = UserGroupInformation.loginUserFromKeytabAndReturnUGI("e3base/e3base01", "C:\\Users\\wzy\\Desktop\\e3base.keytab");
        userGroupInformation.doAs(new PrivilegedAction<Object>() {
            @SneakyThrows
            @Override
            public Object run() {
                try {

                    JSONObject jsonObject = JSON.parseObject(s);
                    System.out.println(jsonObject.getString("errorInfo"));
                    if ("rowDataLack".equals(jsonObject.getString("errorInfo"))) {
                        TableLoader tableLoader = KryoUtils.objectDeCodeC(jsonObject.getString("tableLoader"), TableLoader.class);
                        tableLoader.open();
                        Table table = tableLoader.loadTable();
                        //获取列信息
                        Map<String, Type.TypeID> columnMap = new LinkedHashMap<>();
                        for (Types.NestedField column : table.schema().columns()) {
                            columnMap.put(column.name(), column.type().typeId());
                        }
                        String op = jsonObject.getString("OP");
                        if ("U".equals(op)) {
                            JSONObject beforeJson = jsonObject.getJSONObject("Before");
                            System.out.println(beforeJson.toJSONString());
                            Tuple2<CloseableIterable<Record>, Expression> tuple = getRecord(table, beforeJson, columnMap);
                            //拿到before的存在字段
                            CloseableIterable<Record> recordCloseableIterable = tuple.f0;
                            System.out.println(Iterators.size(recordCloseableIterable.iterator()));
                            if (recordCloseableIterable == null || Iterators.size(recordCloseableIterable.iterator()) != 1) {
                                //如果没有获取到记录，或获取到多条记录，写入侧输出流，进行上报
                                if (jsonObject.containsKey("tableLoader")) {
                                    jsonObject.remove("tableLoader");
                                }
                                ctx.output(errorTag, jsonObject.toJSONString());
                            } else {
                                //根据before拿到的一条记录，结合afterJson，装配成一条新纪录
                                JSONObject afterJson = jsonObject.getJSONObject("After");
                                Record record = recordCloseableIterable.iterator().next();
                                System.out.println(record);
                        /*
                        TODO flinkTableApi预留,可以用于传入IcebergSink
                        RowData rowData = getRowData(afterJson, record, columnMap);
                        TODO 理论上字段如果全的话,无需发送RowKind.UPDATE_BEFORE,待验证
                        rowData.setRowKind(RowKind.UPDATE_AFTER);
                         */

                        /*
                        API操作
                         */
                                DataWriter<GenericRecord> dataWriter = Parquet.writeData(table.io().newOutputFile(table.location() + "/" + UUID.randomUUID().toString()))
                                        .schema(table.schema())
                                        .createWriterFunc(GenericParquetWriter::buildWriter)
                                        .overwrite(true)
                                        .withSpec(PartitionSpec.unpartitioned())
                                        .build();
                                dataWriter.write(
                                        getGenericRecord(
                                                GenericRecord.create(table.schema()),
                                                afterJson,
                                                record,
                                                columnMap));
                                dataWriter.close();
                                //创建事务操作
                                Transaction transaction = table.newTransaction();
                                //delete文件
                                DeleteFiles deleteFiles = transaction.newDelete().deleteFromRowFilter(tuple.f1);
                                System.out.println(tuple.f1);
                                deleteFiles.commit();
                                //dataFile文件
                                DataFile dataFile = dataWriter.toDataFile();
                                AppendFiles appendFiles = transaction.newAppend().appendFile(dataFile);
                                appendFiles.commit();
                                //事务提交
                                transaction.commitTransaction();
                            }
                        } else if ("D".equals(op)) {
                            JSONObject deleteJson = jsonObject.getJSONObject("Delete");
                            //拿到before的存在字段
                            Tuple2<CloseableIterable<Record>, Expression> tuple = getRecord(table, deleteJson, columnMap);
                            CloseableIterable<Record> recordCloseableIterable = tuple.f0;
                            if (recordCloseableIterable == null || Iterators.size(recordCloseableIterable.iterator()) != 1) {
                                //如果没有获取到记录，或获取到多条记录，写入侧输出流，进行上报
                                if (jsonObject.containsKey("tableLoader")) {
                                    jsonObject.remove("tableLoader");
                                }
                                ctx.output(errorTag, jsonObject.toJSONString());
                            } else {
                        /*
                        TODO FlinkTableApi预留，生成RowData
                        RowData rowData = getRowData(deleteJson, record, columnMap);
                        rowData.setRowKind(RowKind.DELETE);
                         */

                                Transaction transaction = table.newTransaction();
                                //delete文件
                                DeleteFiles deleteFiles = transaction.newDelete().deleteFromRowFilter(tuple.f1);
                                deleteFiles.commit();
                                transaction.commitTransaction();
                            }
                        }
                        tableLoader.close();
                    }
                } catch (JSONException e) {
                    e.printStackTrace();
                    ctx.output(errorTag, s);
                } catch (Exception e) {
                    e.printStackTrace();
                    JSONObject jsonObject = JSON.parseObject(s);
                    if (jsonObject.containsKey("tableLoader")) {
                        jsonObject.remove("tableLoader");
                    }
                    ctx.output(errorTag, jsonObject.toJSONString());
                }
                return null;
            }
        });
    }

    /**
     * 获取完整record与获取条件的二元组
     *
     * @param table
     * @param object
     * @param columnMap
     * @return
     */
    private Tuple2<CloseableIterable<Record>, Expression> getRecord(Table table, JSONObject object, Map<String, Type.TypeID> columnMap) {
        IcebergGenerics.ScanBuilder read = IcebergGenerics.read(table);
        Integer size = object.size();
        //避免全表查询
        if (size == 0) {
            return null;
        }
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
        //去除右侧无效值
        Expression[] finalExpressions = Arrays.copyOfRange(expressions, 0, pos.get());
        Expression expression = Expressions.and(Expressions.alwaysTrue(), Expressions.alwaysTrue(), finalExpressions);
        System.out.println(expression);
        CloseableIterable<Record> recordCloseableIterable = read.where(expression)
                .select("*")
                .build();
        return Tuple2.of(recordCloseableIterable, ExpressionUtil.sanitize(expression));
    }

    /**
     * 获取修改后的getGenericRecord对象，用于Iceberg原生Api操作
     *
     * @param genericRecord
     * @param object
     * @param record
     * @param columnMap
     * @return
     */
    private GenericRecord getGenericRecord(GenericRecord genericRecord, JSONObject object, Record record, Map<String, Type.TypeID> columnMap) {
        for (Map.Entry<String, Type.TypeID> entry : columnMap.entrySet()) {
            String column = entry.getKey();
            Type.TypeID type = entry.getValue();
            String jsonValue = object.getString(column);
            if (object.getString(column) == null) {
                genericRecord.setField(column, record.getField(column));
            } else {
                genericRecord.setField(column, caseValueToJavaType(type, jsonValue));
            }
        }
        return genericRecord;
    }

    /**
     * RowData方法，可用于flinkTableAPI转化
     *
     * @param object
     * @param record
     * @param columnMap
     * @return
     */
    private RowData getRowData(JSONObject object, Record record, Map<String, Type.TypeID> columnMap) {
        int pos = 0;
        GenericRowData rowData = new GenericRowData(columnMap.size());
        int pos2 = 0;
        for (Map.Entry<String, Type.TypeID> entry : columnMap.entrySet()) {
            String column = entry.getKey();
            Type.TypeID type = entry.getValue();
            String jsonValue = object.getString(column);
            if (object.getString(column) == null) {
                assembleJavaTypeToRowData(record.getField(column), rowData, pos2);
            } else {
                assembleJavaTypeToRowData(caseValueToJavaType(type, jsonValue), rowData, pos2);
            }
            pos2++;
        }
        return rowData;
    }

    /**
     * icebergType转译成javaType
     *
     * @param typeID
     * @param value
     * @return
     */
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
                return (String) value;
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

    /**
     * 装配RowData
     *
     * @param value
     * @param rowData
     * @param pos
     */
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
}
