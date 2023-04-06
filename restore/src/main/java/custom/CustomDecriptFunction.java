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
import java.util.List;


/**
 * Author: EtherealQ (XQ)
 * Date: 2023/3/20
 * Description:
 * 1.处理异常数据，捕获发送到异常队列(侧输出流 输出)
 * 2.加解密数据
 */

@Slf4j
public class CustomDecriptFunction extends ProcessFunction<String, String> {
    private final OutputTag<String> errorTag;
    private final KafkaConfig kafkaConfig;
    private JSONObject value = null;

    public CustomDecriptFunction(OutputTag<String> errorTag, KafkaConfig kafkaConfig) {
        this.errorTag = errorTag;
        this.kafkaConfig = kafkaConfig;
    }

    @Override
    public void processElement(String str, Context ctx, Collector<String> out) {
        HashSet<String> words = kafkaConfig.decryptFields;
        String decryptKey = kafkaConfig.decryptKey;

        if (null != str) {
            try {
                value = JSON.parseObject(str);
            } catch (JSONException e) {
                //0次为不递归
                handlerJsonException(str, str, 0, ctx);
            }
            if (value == null) {
                //直接返回，异常Json在异常处理函数中发送
                return;
            }
            if (words != null) {
                words.forEach(word -> {
                    if (value.getString("OP").equals("U")) {
                        String aesAfter1 = AESUtils.Decrypt(value.getJSONObject("Before").getString(word), decryptKey);
                        value.getJSONObject("Before").put(word, aesAfter1);
                        String aesAfter2 = AESUtils.Decrypt(value.getJSONObject("After").getString(word), decryptKey);
                        value.getJSONObject("After").put(word, aesAfter2);
                    } else {
                        extracted(word, decryptKey);
                    }
                });
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

    private void extracted(String word, String decryptKey) {
        JSONObject column = getColumn(value);
        if (column != null) {
            String aesBefore = column.getString(word);
            String aesAfter = AESUtils.Decrypt(aesBefore, decryptKey);
            value.put(word, aesAfter);
        }
    }

    /**
     * 处理json解析异常
     * @param str 原始json串，用于发送至侧输出流
     * @param handlerStr    处理后的string，用于多次递归修改
     * @param maxRetry  最大重试次数
     * @param ctx
     */
    private void handlerJsonException(
            String str,
            String handlerStr,
            Integer maxRetry,
            Context ctx) {
        try {
            log.error("Json处理异常，尝试处理....{}", maxRetry);
            handlerStr = handlerStr.replaceAll("[\\s]*\"[\\s]*", "\"");
            char[] temp = handlerStr.toCharArray();
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
            handlerStr = stringBuffer.toString();
            value = JSON.parseObject(handlerStr);
            log.info("异常Json处理成功{}", value.toJSONString());
        } catch (JSONException e) {
            if (maxRetry <= 0) {
                log.error("异常Json处理失败,发送侧输出流---{}", str);
                ctx.output(errorTag, str);
                value = null;
            } else {
                //递归处理
                handlerJsonException(str, handlerStr, maxRetry - 1, ctx);
            }
        } catch (Exception e) {
            log.error("异常Json处理失败,发送侧输出流---{}", str);
            ctx.output(errorTag, str);
            value = null;
        }
    }

}

