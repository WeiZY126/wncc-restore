package custom.test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class Test {
    public static void main(String[] args) throws IOException {
        String str = "{\n" +
                "  \"O:,W:\\”,NER\": \"DSG\",\n" +
                "  \"TABLE\": \"NOSUP_T\",\n" +
                "  \"objnNo\": \"30974\",\n" +
                "  \"OP\": \"I\",\n" +
                "  \"OP_SCN\": \"6180087\",\n" +
                "  \"TRANSACTION_ID\": \"872\",\n" +
                "  \"OP_TIME\": \"2021-02-06 22:43:18\",\n" +
                "  \"LOADERTIME\": \"2021-02-06 22:43:20\",\n" +
                "  \"ORA_ROWID\": \"AAAHJ+A,\"\":\"\"AEAAALMBAAC\",\n" +
                "  \"SEQ_ID\": \"53\",\n" +
                "  \"columnInfo\": {\n" +
                "    \"T_PK\": \"123\",\n" +
                "    \"T0\": \"2021-02-06 22:43:18.000000000\",\n" +
                "    \"T1\": \"zwose0H6IjpP5dt7Nnrl1JL94\",\n" +
                "    \"T2\": \"2021-02-06 22:43:18\",\n" +
                "    \"T3\": \"hbWAwZ0NXBcLta3vTJEu1kSGR\",\n" +
                "    \"T4\": \"BzP9QlLXj0FGTZxmdVbYOurHD\",\n" +
                "    \"T5\": \"2021-02-06 22:43:18.000000000\",\n" +
                "    \"T6\": \"2021-02-06 22:43:18.000000000\",\n" +
                "    \"T7\": \"2021-02-06 22:43:18\",\n" +
                "    \"T8\": \"oPujx5Jh7fvRbqlAesM2Sg8FD\",\n" +
                "    \"T9\": \"8IFQJAMXZqbf0mG\",\n" +
                "    \"T10\": \"652\",\n" +
                "    \"T11\": \"O5hpAwFu9Kc3TdPiBQVSXZHRa\",\n" +
                "    \"T12\": \"pAZh1jDOLKiCTJWvkGmeqg0aI\",\n" +
                "    \"T13\": \"uHzGPcrVYexyURW5M80wEX3AF\",\n" +
                "    \"T14\": \"2021-02-06 22:43:18.000000000\",\n" +
                "    \"T15\": \"2021-02-06 22:43:18\",\n" +
                "    \"T16\": \"2021-02-06 22:43:18.000000000\",\n" +
                "    \"T17\": \"xXgvO\",\n" +
                "    \"T18\": \"pH9kn\",\n" +
                "    \"T19\": \"TBQyxkrmKvfpuMaFqdWj0NJ2R\"\n" +
                "  },\n" +
                "  \"PK\": {\n" +
                "    \"T_PK\": \"123\"\n" +
                "  }\n" +
                "}";
        testFastJson(str);
//        testJackSon(str);
    }

    private static void testFastJson(String str) {
        JSONObject jsonObject = JSON.parseObject(str);
        System.out.println(jsonObject.toJSONString());
    }
    private static void testJackSon(String str) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode = objectMapper.readTree(str);
        System.out.println(jsonNode.toString());
    }
}
