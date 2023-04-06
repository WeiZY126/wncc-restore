import config.EnvConfig;
import config.IcebergConfig;
import config.KafkaConfig;
import custom.CustomDecriptFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.OutputTag;
import utils.Parameter;

/**
 * Author: EtherealQ (XQ)
 * Date: 2023/3/6
 * Description:全网移动项目 kafka upset入湖
 */
public class mainFunction {
    private final static OutputTag<String> ERROR_DATE = new OutputTag<>("error-date", TypeInformation.of(String.class));

    public static void main(String[] args) throws Exception {

        String[] arg = new String[2];
        arg[0] = "--path";
        arg[1] = "D:/sitech/notepad/P类项目/移动全网客户中心/还原层/黑龙江/CT_CUST_INFO.json";

        //从启动参数中获取参数
        final ParameterTool parameterTool = ParameterTool.fromArgs(arg);
        Parameter parameter = new Parameter(parameterTool);
        //获取执行环境
        final StreamExecutionEnvironment env = EnvConfig.getEnv();
        KafkaConfig kafkaConfig = new KafkaConfig(parameter.path);
        IcebergConfig icebergConfig = new IcebergConfig(parameter.path);
        //消费kafka
        SingleOutputStreamOperator<String> stream = kafkaConfig.getDataStream(env);
        //stream.print("kafka stream");
        // 解密
        SingleOutputStreamOperator<String> decriptStream = stream
                .process(new CustomDecriptFunction(ERROR_DATE, kafkaConfig));
        // 加密
        SingleOutputStreamOperator<String> encriptStream = decriptStream
                .process(new CustomDecriptFunction(ERROR_DATE, kafkaConfig));
        encriptStream.print("data");

        SingleOutputStreamOperator<RowData> sinkToIceberg = icebergConfig.setSinkTo(encriptStream, parameter.writeParallelism, ERROR_DATE);
        DataStream<String> errorStream = encriptStream.getSideOutput(ERROR_DATE).union(sinkToIceberg.getSideOutput(ERROR_DATE));


        // TODO 预留位置 处理异常数据
        //errorStream.print("error");
        kafkaConfig.setErrorSinkTo(errorStream);
        //写入kafka指定分区
        //kafkaConfig.setSinkTo(encriptStream);
        //env.execute(icebergConfig.tuple2.f0.loadTable().name());
        env.execute();

    }

}