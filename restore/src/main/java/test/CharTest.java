package test;

import com.alibaba.fastjson.JSONObject;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class CharTest {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.addResource(new File("C:\\Users\\wzy\\Desktop\\数据集成平台\\wncc-restore\\restore\\ExceptionHandler\\src\\main\\resources\\core-site.xml").toURI().toURL());
        configuration.addResource(new File("C:\\Users\\wzy\\Desktop\\数据集成平台\\wncc-restore\\restore\\ExceptionHandler\\src\\main\\resources\\hdfs-site.xml").toURI().toURL());

        TableLoader tableLoader = TableLoader.fromHadoopTable("hdfs://drmcluster/iceberg/hadoop/warehouse/restore_catalog/testdb/PERSON", configuration);
//        tableLoader.open();
//        System.out.println(tableLoader.loadTable().name());

        String str = "{\n" +
                "  \"OWNER\": \"DSG\",\n" +
                "  \"TABLE\": \"NOSUP_T\",\n" +
                "  \"objnNo\": \"30974\",\n" +
                "  \"OP\": \"U\",\n" +
                "  \"OP_SCN\": \"6181400\",\n" +
                "  \"TRANSACTION_ID\": \"872\",\n" +
                "  \"OP_TIME\": \"2021-02-06 22:44:42\",\n" +
                "  \"LOADERTIME\": \"2021-02-06 22:44:46\",\n" +
                "  \"ORA_ROWID\": \"AAAHJ+AAEAAALL0AAO\",\n" +
                "  \"SEQ_ID\": \"61\",\n" +
                "  \"After\": {\n" +
                "    \"NAME\": \"AK33\"\n" +
                "  },\n" +
                "  \"Before\": {\n" +
                "    \"NAME\": \"AK11\"\n" +
                "  },\n" +
                "  \"PK\": {\n" +
                "  }\n" +
                "}";
        JSONObject jsonObject = JSONObject.parseObject(str);
        Kryo kryo = new Kryo();
        kryo.setReferences(false);
        kryo.register(tableLoader.getClass(), new JavaSerializer());
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Output output = new Output(baos);
        kryo.writeClassAndObject(output, tableLoader);
        output.flush();
        output.close();
        byte[] bytes = baos.toByteArray();
        try {
            baos.flush();
            baos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        jsonObject.put("errorInfo", "rowDataLack");
        jsonObject.put("tableLoader", new String(new Base64().encode(bytes)));
        kafkaProducer(jsonObject.toJSONString());
    }

    public static void kafkaProducer(String str) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        //设置Kafka服务器地址
        props.put("bootstrap.servers", "172.21.9.101:9092,172.21.9.102:9092,172.21.9.103:9092");
        //设置数据key的序列化处理类
        props.put("key.serializer", StringSerializer.class.getName());
        //设置数据value的序列化处理类
        props.put("value.serializer", StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        producer.send(new ProducerRecord<>("error_topic_test0406", 1, "", str)).get();
    }
}
