package flink.test;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import ru.yandex.clickhouse.BalancedClickhouseDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.*;

public class Tockjson3 {
    public static void main(String[] args) throws Exception {
        //TODO 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //TODO 1.source
        //准备kafka连接参数
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "10.10.41.242:9092");//集群地址
        props.setProperty("bootstrap.servers", "10.10.41.243:9092");//集群地址
        props.setProperty("bootstrap.servers", "10.10.41.251:9092");//集群地址
        props.setProperty("group.id", "flink");//消费者组id
        props.setProperty("auto.offset.reset", "latest");//latest有offset记录从记录位置开始消费,没有记录从最新的/最后的消息开始消费 /earliest有offset记录从记录位置开始消费,没有记录从最早的/最开始的消息开始消费
        //使用连接参数创建FlinkKafkaConsumer/kafkaSource
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<String>("da_trace", new SimpleStringSchema(), props);
        //使用kafkaSource
        DataStream<String> kafkaDS = env.addSource(kafkaSource);
        //TODO 2.transformation

        //1,将kafka,String数据变更成jsonobj并打上时间戳time和UUID
        DataStream<JSONObject> dataStream = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {

            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    jsonObject.put("uuid", UUID.randomUUID());
                    jsonObject.put("time",new Date());
                    out.collect(jsonObject);
            }
        });
        //2,将数据拆分成两个流,1:伪装和标签,2:变形
        OutputTag<JSONObject> camouflageandlabel = new OutputTag<JSONObject>("伪装和标签", TypeInformation.of(JSONObject.class));
        OutputTag<JSONObject> transformation = new OutputTag<JSONObject>("变形",TypeInformation.of(JSONObject.class)){};
        SingleOutputStreamOperator<JSONObject> process = dataStream.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject jsonObject, Context context, Collector<JSONObject> collector) throws Exception {
                //获取到数据体basicMessageBasicList
                JSONArray basicMessageBasicList = jsonObject.getJSONArray("basicMessageBasicList");
                Map map = new HashMap();
                //遍历告警JSON
                for (int i = 0; i < basicMessageBasicList.size(); i++) {
                    map = basicMessageBasicList.getJSONObject(i);
                    //获取所有key的Set集合
                    Set<String> keySet = map.keySet();
                    //获取key的Set集合的迭代器
                    Iterator<String> it = keySet.iterator();
                    //判断迭代器中是否还有元素,如果有,则进入循环;如果没有,则停止。
                    while (it.hasNext()) {
                        if (map.get("verifyFunctionModuleCode").equals(6)) {
                            context.output(transformation, jsonObject);
                        } else {
                            context.output(camouflageandlabel, jsonObject);
                        }
                    }
                }
            }
        });
        DataStream<JSONObject> camouflageandlabel1 = process.getSideOutput(camouflageandlabel);
        DataStream<JSONObject> transformation2 = process.getSideOutput(transformation);
        camouflageandlabel1.print();
        transformation2.print();

        //dataStream.print();
        //dataStream.addSink(new ckSink());
        env.execute();
    }

    private static class ckSink extends RichSinkFunction<JSONObject> {
        String sql= "";
        private Statement stmt;
        private  Connection conn;
        private PreparedStatement preparedStatement;
        String jdbcUrl = "jdbc:clickhouse://10.10.41.251:8123/default";//39.96.136.60:8123,,10.10.41.242:8123,10.10.41.251:8123
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            //加载JDBC驱动
            Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
            //获取数据库连接
            conn = new BalancedClickhouseDataSource(jdbcUrl).getConnection("default", "123456");
            stmt = conn.createStatement();
            //preparedStatement = conn.prepareStatement(sql);
        }
        @Override
        public void close() throws Exception {
            super.close();
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }
        @Override
        public void invoke(JSONObject value, Context context) throws Exception {
            String sql= "";
            StringBuilder columns = new StringBuilder("messageProtocolVersion,messageDeviceTypeId,messageProductId,messageDeviceDescribe,messageEmbeddedSoftwareVersion," +
                    "messageChipVersion,messageDeviceSerialId,messagePackageId,messageLoadLength,messageNumber,messageSplitSerialId," +
                    "verifyCode,reserved,");
            StringBuilder values = new StringBuilder();
            values.append("'");
            values.append (value.getOrDefault("messageProtocolVersion","-1")).append("','");
            values.append (value.getOrDefault("messageDeviceTypeId","-1")).append("','");
            values.append (value.getOrDefault("messageProductId","-1")).append("','");
            values.append (value.getOrDefault("messageDeviceDescribe","-1")).append("','");
            values.append (value.getOrDefault("messageEmbeddedSoftwareVeeVersion","-1")).append("','");
            values.append (value.getOrDefault("messageChipVersion","-1")).append("','");
            values.append (value.getOrDefault("messageDeviceSerialId","-1")).append("','");
            values.append (value.getOrDefault("messagePackageId","-1")).append("','");
            values.append (value.getOrDefault("messageLoadLength","-1")).append("','");
            values.append (value.getOrDefault("messageNumber","-1")).append("','");
            values.append (value.getOrDefault("messageSplitSerialId","-1")).append("','");
            values.append (value.getOrDefault("verifyCode","-1")).append("','");
            values.append ("-1").append("','");
            try {
                //获取所有告警的JSON
                JSONArray basicMessageBasicList = value.getJSONArray("basicMessageBasicList");
                Map map = new HashMap();
                //遍历告警JSON
                for (int i = 0; i < basicMessageBasicList.size(); i++) {
                    map = basicMessageBasicList.getJSONObject(i);
                    //遍历第 i 个json
                    Set<Map.Entry<String, String>> set = map.entrySet();
                    for (Map.Entry<String, String> stringStringEntry : set) {
                        columns.append(stringStringEntry.getKey().toString()).append(",");
                        String s = String.valueOf(stringStringEntry.getValue());
                        values.append(s).append("','");
                    }
                    columns.append("tempVerifyCode,messageSplit");
                    values.append (value.getOrDefault("tempVerifyCode","-1")).append("','");
                    values.append (value.getOrDefault("messageSplit","-1")).append("'");
                    String sqlkey = columns.toString();
                    String sqlvalue = values.toString();
                    sql = "INSERT INTO default.report ( "+ sqlkey+" ) VALUES ( "+sqlvalue +" )";
                    System.out.println(sql);
                    stmt.executeQuery(sql);
                }
            }catch (Exception e){
                System.out.println(e);
            }
        }
    }
}
