package com.sitech;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

import com.alibaba.fastjson.JSONObject;
import com.sitech.config.EnvConfig;
import com.sitech.config.KafkaConfig;
import com.sitech.custom.ExceptionHandlerProcess;
import com.sitech.utils.Parameter;

public class ExcptionHandlerRun {
    private final static OutputTag<String> ERROR_DATE = new OutputTag<>("error-date", TypeInformation.of(String.class));

    public static void main(String[] args1) throws Exception {
        String[] args = new String[2];
        args[0] = "--path";
        args[1] = "C:\\Users\\wzy\\Desktop\\数据集成平台\\wncc-restore\\restore\\ExceptionHandler\\src\\main\\resources\\errorHandler.json";
        //从启动参数中获取参数
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        Parameter parameter = new Parameter(parameterTool);
        //获取执行环境
        final StreamExecutionEnvironment env = EnvConfig.getEnv();
        KafkaConfig kafkaConfig = new KafkaConfig(parameter.path);
        //消费kafka
        SingleOutputStreamOperator<String> stream = kafkaConfig.getDataStream(env);
        SingleOutputStreamOperator<JSONObject> exceptionHandler = stream.process(new ExceptionHandlerProcess(ERROR_DATE));
        DataStream<String> errorData = exceptionHandler.getSideOutput(ERROR_DATE);
        kafkaConfig.setErrorSinkTo(errorData);

        env.execute("error-handler");

    }
}
