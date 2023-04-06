package test;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.annotation.JSONField;
import com.alibaba.fastjson.serializer.JSONSerializer;
import com.alibaba.fastjson.serializer.ObjectSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringEscapeUtils;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class kafkaReaderJsonTest {

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
        source.map(str -> {
            Long startTime = System.currentTimeMillis();
            System.out.println("开始处理异常json,原json串:" + str);
            JSONObject jsonObject = null;

            str = handlerException(str);
            Double useTime = (double) (System.currentTimeMillis() - startTime) / 1000;
//            Long useTime = System.currentTimeMillis() - startTime;
            try {
                jsonObject = JSON.parseObject(str);
                System.out.println("parseObject解析成功,解析后jsonObject:" + jsonObject.toJSONString());
                System.out.println("本次处理耗时 " + useTime + " sec");
            } catch (JSONException e) {
                System.out.println(e.getMessage());
                log.error(str, e);
            }
            return jsonObject;
        });

        env.execute();

    }

    public static String handlerException(String str) {
        str = str.replaceAll("[\\s]*\"[\\s]*", "\"");
        char[] temp = str.toCharArray();
        int n = temp.length;
        StringBuffer stringBuffer = new StringBuffer(n);
        stringBuffer.append("{");
        for (int i = 1; i < n; i++) {
            if (temp[i - 1] == '"' && temp[i] == ':' && temp[i + 1] == '"') {
                for (int j = i + 2; j < n; j++) {
                    if (temp[j] == '"') {
                        if (temp[j + 1] != ',' && temp[j + 1] != '}') {
                            temp[j] = '‘';
                        } else if (temp[j + 1] == ',' || temp[j + 1] == '}') {
                            //如果包含xxx"}xxx与xxx",xxx的情况
                            if (j + 2 < n && Character.isLetterOrDigit(temp[j + 2])) {
                                temp[j] = '‘';
                            }
                            break;
                        }
                    }
                }
            }
            //判断反斜杠
            if (temp[i] == '\\') {
                char c = temp[i + 1];
                if (Character.isDigit(c)) {
                    if (c > '7') {
                        stringBuffer.append("\\");
                    }
                } else if (Character.isLowerCase(c)) {
                    if (c != 'b' && c != 'n' && c != 'r' && c != 't') {
                        stringBuffer.append("\\");
                    }
                } else {
                    if (c != '\\' && c != '\'' && c != '"') {
                        stringBuffer.append("\\");
                    }
                    //跳过下个斜杠扫描
                    if (c == '\\') {
                        stringBuffer.append(temp[i++]);
                    }
                }
            }
            stringBuffer.append(temp[i]);
        }
        return stringBuffer.toString();
    }

    class StringToJsonSerializer implements ObjectSerializer {

        @Override
        public void write(JSONSerializer serializer, Object object, Object fieldName, Type fieldType,
                          int features) throws IOException {
            serializer.write(JSONObject.parseObject(object.toString()));
        }

    }
}
