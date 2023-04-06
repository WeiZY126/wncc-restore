package custom;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import lombok.SneakyThrows;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.nio.charset.StandardCharsets;

/**
 * Author: EtherealQ (XQ)
 * Date: 2023/3/13
 * Description: 异常数据处理: 表值内容非正常数据(",?,等等)
 */
public class CustomJsonSchema implements DeserializationSchema<JSONObject>, SerializationSchema<JSONObject> {
    @Override
    public JSONObject deserialize(byte[] message) {
        String value = new String(message, StandardCharsets.UTF_8);
        JSONObject jsonObject = null;
        try {
            jsonObject = JSON.parseObject(value);
        } catch (JSONException e) {
            //TODO 把不能正常解析成json的数据过滤
            System.out.print("此数据JSON解析失败: \n" + value);
        }
        return jsonObject;
    }

    @Override
    public boolean isEndOfStream(JSONObject nextElement) {
        return false;
    }

    @SneakyThrows
    @Override
    public byte[] serialize(JSONObject element) {
        return element.toString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public TypeInformation<JSONObject> getProducedType() {
        return TypeInformation.of(JSONObject.class);
    }
}