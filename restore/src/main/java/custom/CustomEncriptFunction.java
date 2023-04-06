package custom;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import config.KafkaConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import utils.AESUtils;

import java.util.HashSet;


/**
 * Author: EtherealQ (XQ)
 * Date: 2023/3/20
 * Description:
 * 1.处理异常数据，捕获发送到异常队列(侧输出流 输出)
 * 2.加解密数据
 */

@Slf4j
public class CustomEncriptFunction extends ProcessFunction<String, String> {
    private final OutputTag<String> errorTag;
    private final KafkaConfig kafkaConfig;
    private JSONObject value = null;

    public CustomEncriptFunction(OutputTag<String> errorTag, KafkaConfig kafkaConfig) {
        this.errorTag = errorTag;
        this.kafkaConfig = kafkaConfig;
    }

    @Override
    public void processElement(String str, Context ctx, Collector<String> out) {
        HashSet<String> encryptFields = kafkaConfig.encryptFields;
        String encryptKey = kafkaConfig.encryptKey;

        if (null != str) {
            try {
                value = JSON.parseObject(str);
                if (encryptFields != null) {
                    encryptFields.forEach(word -> {
                        if (value.getString("OP").equals("U")) {
                            String aesAfter1 = AESUtils.Encrypt(value.getJSONObject("Before").getString(word),encryptKey);
                            value.getJSONObject("Before").put(word, aesAfter1);
                            String aesAfter2 = AESUtils.Encrypt(value.getJSONObject("After").getString(word),encryptKey);
                            value.getJSONObject("After").put(word, aesAfter2);
                        } else {
                            extracted(word,encryptKey);
                        }
                    });
                }
            } catch (JSONException e) {
                //把不能正常解析成json的数据过滤
                //log.error("此数据JSON解析失败: \n" + str);
                ctx.output(errorTag, str);
            }
        }
        if (value != null) {
            out.collect(value.toString());
        }
    }


    /**
     * AES加解密数据
     *
     * @return 按规则加解密之后的数据
     */
    private String dataAES(String data) {
        // TODO 加解密函数添加
        return data;
    }

    private JSONObject getColumn(JSONObject jsonObject) {
        String op = jsonObject.getString("OP");
        JSONObject columnInfo;
        if (op.equals("I")) {
            columnInfo = jsonObject.getJSONObject("columnInfo");
            return columnInfo;
        }
        if (op.equals("D")) {
            columnInfo = jsonObject.getJSONObject("Before");
            return columnInfo;
        }
        return null;
    }

    private void extracted(String word,String encryptKey) {
        JSONObject column = getColumn(value);
        if (column != null) {
            String aesBefore = column.getString(word);
            String aesAfter = AESUtils.Decrypt(aesBefore,encryptKey);
            value.put(word, aesAfter);
        }
    }
}

