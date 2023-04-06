package com.sitech.config;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;

import com.alibaba.fastjson.JSONObject;
import com.sitech.utils.ParseJSON2MapUtils;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.common.TopicPartition;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * Author: EtherealQ (XQ)
 * Date: 2023/3/8
 * Description: 映射kafka配置json
 */

@Slf4j
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class KafkaConfig implements Serializable {
    private KafkaSource<String> kafkaSource;
    private KafkaSink<String> kafkaSink;
    private KafkaSink<String> kafkaErrorSink;
    private String path;
    public String decryptKey;
    public String encryptKey;
    public HashSet<String> encryptFields;
    public HashSet<String> decryptFields;

    /**
     * Description: 初始化配置文件
     */
    public KafkaConfig(String path) {
        this.path = path;
        File file = new File(path);
        String str = null;
        try {
            str = FileUtils.readFileToString(file, "UTF-8");
        } catch (IOException e) {
            // log.error("Error reading" + path + "file");
        }
        Map<String, Object> mapForJson = ParseJSON2MapUtils.parseJSON2Map(str);
        JSONObject kafkaSource = (JSONObject) mapForJson.get("kafka-source");
        JSONObject kafkaSink = (JSONObject) mapForJson.get("kafka-sink");
        JSONObject kafkaErrorSink = (JSONObject) mapForJson.get("kafka-error");

        this.decryptKey = kafkaSource.getString("decryptKey");
        this.encryptKey = kafkaSink.getString("encryptKey");
        this.kafkaSource = initKafkaSource(kafkaSource);
        this.kafkaSink = initKafkaSink(kafkaSink);
        this.kafkaErrorSink = initKafkaSink(kafkaErrorSink);

    }

    private KafkaSource<String> initKafkaSource(JSONObject js) {
        KafkaSource<String> consumer;
        KafkaSourceBuilder<String> builder = KafkaSource.builder();
        builder
                .setBootstrapServers(js.getString("bootstrapServers"))
                .setGroupId(js.getString("groupId"))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setPartitions(new HashSet<>(Collections.singletonList(new TopicPartition(js.getString("topic"), js.getIntValue("partitionId")))));
        String startingOffsets = js.getString("startingOffsets").toLowerCase();
        switch (startingOffsets) {
            case "earliest":
                consumer = builder.setStartingOffsets(OffsetsInitializer.earliest()).build();
                break;
            case "latest":
                consumer = builder.setStartingOffsets(OffsetsInitializer.latest()).build();
                break;
            case "timestamp":
                consumer = builder.setStartingOffsets(OffsetsInitializer.timestamp(js.getLongValue("timeStamp"))).build();
                break;
            case "offsets":
                Map<TopicPartition, Long> map = new HashMap<>();
                map.put(new TopicPartition(js.getString("topic"), js.getIntValue("partitionId")), js.getLongValue("offSets"));
                consumer = builder.setStartingOffsets(OffsetsInitializer.offsets(map)).build();
                break;
            case "committedoffsets":
                consumer = builder.setStartingOffsets(OffsetsInitializer.committedOffsets()).build();
                break;
            default:
                throw new UnsupportedOperationException(startingOffsets + " mode is not supported. Check startingOffsets is available");
        }
        return consumer;
    }

    private KafkaSink<String> initKafkaSink(JSONObject js) {
        return KafkaSink.<String>builder()
                .setBootstrapServers(js.getString("bootstrapServers"))
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(js.getString("topic"))
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .setPartitioner(new FlinkKafkaPartitioner<String>() {
                            @Override
                            public int partition(String record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
                                return js.getIntValue("partitionId");
                            }
                        }).build())
                .build();
        //TODO 规范一下数值，避免数据丢失
        // acks
        // log.flush.interval.messages
        // log.flush.interval.ms
        // log.flush.*
    }





    public SingleOutputStreamOperator<String> getDataStream(StreamExecutionEnvironment env) {
        return env.fromSource(
                this.kafkaSource,
                WatermarkStrategy.noWatermarks(), "Kafka-Source");
    }

    public void setSinkTo(DataStream<String> stream) {
        stream.sinkTo(this.kafkaSink).name("kafka-sink");
    }

    public void setErrorSinkTo(DataStream<String> stream) {
        stream.sinkTo(this.kafkaErrorSink).name("kafka-error-sink");
    }
}