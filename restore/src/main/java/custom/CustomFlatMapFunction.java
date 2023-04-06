package custom;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;

import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Author: EtherealQ (XQ)
 * Date: 2023/3/8
 * Description: 解析json
 */
@Slf4j
public class CustomFlatMapFunction extends ProcessFunction<String, RowData> {
    private final JsonRowDataDeserializationSchema jrd;
    private final OutputTag<String> errorTag;
    private TableLoader tableLoader;

    public CustomFlatMapFunction(JsonRowDataDeserializationSchema jrd, OutputTag<String> errorTag, TableLoader tableLoader) {
        this.jrd = jrd;
        this.errorTag = errorTag;
        this.tableLoader = tableLoader;
    }

    @Override
    public void processElement(String str, Context ctx, Collector<RowData> out) {
        if (str != null) {
            JSONObject jsonObject = JSON.parseObject(str);
            String op = jsonObject.getString("OP");
            try {
                if (op.equals("I")) {
                    RowData rowData = getRowData(jsonObject, "columnInfo");
                    rowData.setRowKind(RowKind.INSERT);
                    int rowDataLength = jsonObject.getJSONObject("columnInfo").size();
                    emitWithNoTableLoader(ctx, out, jsonObject, rowData, rowDataLength);
                } else if (op.equals("U")) {
                    RowData rowData1 = getRowData(jsonObject, "Before");
                    rowData1.setRowKind(RowKind.UPDATE_BEFORE);
                    int rowDataLength1 = jsonObject.getJSONObject("Before").size();
                    emitWithNoTableLoader(ctx, out, jsonObject, rowData1, rowDataLength1);
                    RowData rowData2 = getRowData(jsonObject, "After");
                    rowData2.setRowKind(RowKind.UPDATE_AFTER);
                    int rowDataLength2 = jsonObject.getJSONObject("After").size();
                    emit(ctx, out, jsonObject, rowData2, rowDataLength2);
                } else if (op.equals("D")) {
                    RowData rowData = getRowData(jsonObject, "Before");
                    int rowDataLength = jsonObject.getJSONObject("Before").size();
                    rowData.setRowKind(RowKind.DELETE);
                    emit(ctx, out, jsonObject, rowData, rowDataLength);
                }
            } catch (Exception e) {
                //log.error(e.getMessage());
                ctx.output(errorTag, jsonObject.toString());
            }
        }
    }

    private RowData getRowData(JSONObject jsonObject, String mode) throws IOException {
        RowData rowData;
        rowData =
                jrd.deserialize(jsonObject.getString(mode).getBytes(StandardCharsets.UTF_8));
        return rowData;
    }

    /**
     * 侧输出流需判断的类
     * @param ctx
     * @param out
     * @param jsonObject
     * @param rowData
     * @param rowDataLength
     */
    private void emit(ProcessFunction<String, RowData>.Context ctx, Collector<RowData> out, JSONObject jsonObject, RowData rowData, Integer rowDataLength) {
        Boolean isError = false;
        for (Integer i = 0; i < rowDataLength; i++) {
            if (rowData.isNullAt(i)) {
                isError = true;
                break;
            }
        }
        //如果有列为空
        if (isError) {
            jsonObject.put("errorInfo", "rowDataLack");
            jsonObject.put("tableLoader", tableLoader);
            ctx.output(errorTag, jsonObject.toString());
        } else {
            out.collect(rowData);
        }
    }

    /**
     * 侧输出流不加tableloader，主要是insert、updateBefore，只判断关键字段，传入后不进行处理
     *
     * @param ctx
     * @param out
     * @param jsonObject
     * @param rowData
     * @param rowDataLength
     */
    private void emitWithNoTableLoader(ProcessFunction<String, RowData>.Context ctx, Collector<RowData> out, JSONObject jsonObject, RowData rowData, Integer rowDataLength) {
        if (rowData != null && !rowData.isNullAt(0)) {
            out.collect(rowData);
            return;
        } else {
            jsonObject.put("errorInfo", "InsertOrUpBeforeRowDataLack");
            ctx.output(errorTag, jsonObject.toString());
        }
    }
}