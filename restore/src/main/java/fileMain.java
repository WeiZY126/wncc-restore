import com.alibaba.fastjson.JSONObject;
import config.EnvConfig;
import config.IcebergConfig;
import config.KafkaConfig;
import custom.CustomDecriptFunction;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.OutputTag;
import utils.Parameter;

import java.io.File;
import java.io.IOException;

public class fileMain {
    private final static OutputTag<String> ERROR_DATE = new OutputTag<>("error-date", TypeInformation.of(String.class));

    public static void main(String[] args) throws Exception{
        final StreamExecutionEnvironment env = EnvConfig.getEnv();

        String[] arg = new String[4];
        arg[0] = "--path";
        arg[1] = "hdfs://xardc2:12001/tmp/";
        arg[2] = "--fieldsPath";
        arg[3] = "D:/sitech/notepad/P类项目/移动全网客户中心/还原层/黑龙江/INF_CUSTOMER_field.txt";
        ParameterTool parameterTool = ParameterTool.fromArgs(arg);
        Parameter parameter = new Parameter(parameterTool);

        KafkaConfig kafkaConfig = new KafkaConfig(parameter.path);
        //InfoConfig infoConfig = new InfoConfig(parameter.path);
        IcebergConfig icebergConfig = new IcebergConfig(parameter.path);

        String[] fields = getFields(parameter.fieldsPath);

        SingleOutputStreamOperator<String> filesource = env.readTextFile(parameter.path);
        SingleOutputStreamOperator<String> stream = mapfields(filesource, fields);
        // 解密
        SingleOutputStreamOperator<String> decriptStream = stream
                .process(new CustomDecriptFunction(ERROR_DATE, kafkaConfig));
        // 加密
        SingleOutputStreamOperator<String> encriptStream = decriptStream
                .process(new CustomDecriptFunction(ERROR_DATE, kafkaConfig));
        //filterStream.print("data");

        SingleOutputStreamOperator<RowData> sinkToIceberg = icebergConfig.setSinkTo(encriptStream, parameter.writeParallelism, ERROR_DATE);
        DataStream<String> errorStream =
                encriptStream.getSideOutput(ERROR_DATE)
                        .union(sinkToIceberg.getSideOutput(ERROR_DATE));
        // TODO 预留位置 处理异常数据
        //errorStream.print("error");
        kafkaConfig.setErrorSinkTo(errorStream);
        //写入kafka指定分区
        kafkaConfig.setSinkTo(encriptStream);
        env.execute(icebergConfig.tuple2.f0.loadTable().name());

    }

    public static String[] getFields(String path){
        File file = new File(path);
        String fields = null;
        try {
            fields = FileUtils.readFileToString(file, "UTF-8");
        } catch (IOException e) {
            //log.error("Error reading" + path + "file");
        }
        String[] splits = fields.split(",");

        return splits;
    }

    public static SingleOutputStreamOperator<String>  mapfields(SingleOutputStreamOperator<String> filesource,  String[] fields){
        SingleOutputStreamOperator<String> stream = filesource.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                JSONObject jsonObject = new JSONObject();
                JSONObject jsonValueObject = new JSONObject();
                String[] values = s.trim().split(",");
                for(int i=0;i<fields.length;i++){
                    jsonValueObject.put(fields[i].trim().toUpperCase(), values[i].trim());
                }
                jsonObject.put("OP","I");
                jsonObject.put("columnInfo",jsonValueObject);
                return jsonObject.toString();
            }
        });
        return stream;
    }

}
