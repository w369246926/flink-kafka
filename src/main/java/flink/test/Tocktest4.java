package flink.test;

import cn.hutool.core.util.IdUtil;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.errors.MinioException;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import ru.yandex.clickhouse.BalancedClickhouseDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class Tocktest4 {
    public static void main(String[] args) throws Exception {
        //TODO 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //TODO 1.source
        //准备kafka连接参数
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "192.168.88.161:9092");//集群地址
        props.setProperty("bootstrap.servers", "192.168.88.162:9092");//集群地址
        props.setProperty("bootstrap.servers", "192.168.88.163:9092");//集群地址
        props.setProperty("group.id", "flink");//消费者组id
        props.setProperty("auto.offset.reset", "latest");//latest有offset记录从记录位置开始消费,没有记录从最新的/最后的消息开始消费 /earliest有offset记录从记录位置开始消费,没有记录从最早的/最开始的消息开始消费
        //使用连接参数创建FlinkKafkaConsumer/kafkaSource
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<String>("matedata", new SimpleStringSchema(), props);
        //使用kafkaSource
        DataStream<String> kafkaDS = env.addSource(kafkaSource);
        //TODO 2.transformation
        DataStream<JSONObject> dataStream = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                //有大括号[json]
                if (s.substring(0, 1).equals("[")) {
                    String substring = s.substring(1, (s.length()-2));
                    String replace = substring.replace("[", "\"").replace("]","\"");
                    System.out.println("yes[]----" + replace);
                    JSONObject jsonObject = JSONObject.parseObject(replace);
                    collector.collect(jsonObject);
                } else {
                    System.out.println("no[]----" + s);
                    String replace = s.replace("[", "\"").replace("]","\"");
                    JSONObject jsonObject = JSONObject.parseObject(replace);
                    collector.collect(jsonObject);
                }
                //无大括号json
            }
        });


        //TODO 3.sink


        dataStream.print();
        dataStream.addSink(new ckSink());


        env.execute();

    }

    private static class ckSink extends RichSinkFunction<JSONObject> {
        String sql= "";
        private Statement stmt;
        private  Connection conn;
        private PreparedStatement preparedStatement;
        String jdbcUrl = "jdbc:clickhouse://192.168.88.161:8123/default";//39.96.136.60:8123,,10.10.41.242:8123,10.10.41.251:8123
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
            try{
                //获取所有告警的JSON
                    StringBuilder columns = new StringBuilder();
                    StringBuilder values = new StringBuilder();
                    values.append("'");
                    //遍历第 i 个json
                    Set<Map.Entry<String, Object>> set = value.entrySet();
                    for (Map.Entry<String, Object> stringStringEntry : set) {
                        columns.append(stringStringEntry.getKey().toString()).append(",");
                        String s = String.valueOf(stringStringEntry.getValue());
                        if (s.equals(null)){
                            s = "0";
                        }
                        String s1 = s.replace("'", "|");

                        values.append(s1).append("','");
                    }
                    columns.append("uuid");
                    values.append (IdUtil.simpleUUID()).append("'");;
//                    values.append (value.getOrDefault("messageSplit","-1")).append("'");
                    String sqlkey = columns.toString();
                    String sqlvalue = values.toString();
                    sql = "INSERT INTO default.Matedata ( "+ sqlkey+" ) VALUES ( "+sqlvalue +" )";
                    //System.out.println("A表");
                    stmt.executeQuery(sql);

            }catch (Exception e){
                System.out.println(e);
            }
        }
    }

}
