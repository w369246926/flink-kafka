package flink.test;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.calcite.shaded.com.fasterxml.jackson.annotation.JsonAlias;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import ru.yandex.clickhouse.BalancedClickhouseDataSource;
import java.sql.Connection;
import org.apache.flink.table.api.Types;
import java.sql.PreparedStatement;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class Tock2 {
    public static void main(String[] args) throws Exception {
        /**
         * 样例数据:
         {"userId":"1","day":"2020-01-05","data":[{"package":"com.zyd","activetime":"2311"}]}
         {"messageProtocolVersion":"","messageDeviceTypeId":0,"messageProductId":0,"messageDeviceDescribe":"","messageEmbeddedSoftwareVersion":"","messageChipVersion":"","messageDeviceSerialId":"JAJX01710001C100","messagePackageId":0,"messageLoadLength":0,"messageNumber":1,"messageSplitSerialId":0,"verifyCode":0,"reserved":0,"basicMessageBasicList":[{"srcMac":"11-22-33-aa-44-55","destMac":"10-20-30-40-50-60","srcIp":"10.10.10.100","destIp":"10.10.10.110","srcPort":8080,"destPort":8090,"protocolType":0,"byteNumber":-31395,"packageNumber":21773,"verifyMessageBodyType":1,"verifyTypeId":101,"loadLength":31,"verifyFunctionModuleCode":0}],"tempVerifyCode":0,"messageSplit":false}
         */
        //TODO 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        StreamTableEnvironment sTableEnv = StreamTableEnvironment.create(env);

        //TODO 1.source
        sTableEnv.connect(new Kafka()
                .version("universal")
                .topic("da_trace")
                .startFromLatest()
                //.property("group.id", "flink")
                //.property("bootstrap.servers", "10.10.41.242:9092,10.10.41.243:9092,10.10.41.251:9092")
                .property(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"10.10.41.251:9092")
                .property(ConsumerConfig.GROUP_ID_CONFIG,"001")
        ).withFormat(
                new Json()
                        //.failOnMissingField(true)
                        //.deriveSchema()
        ).withSchema(
                new Schema()
                        .field("userId", Types.LONG()) //一层嵌套json
                        .field("day", Types.STRING())
                        .field("data", ObjectArrayTypeInfo.getInfoFor(
                                Row[].class,
                                Types.ROW(
                                        new String[]{"package", "activetime"},
                                        new TypeInformation[] {Types.STRING(), Types.LONG()}
                                )
                        ))
        ).createTemporaryTable("report");
        Table report = sTableEnv.from("report");
//        Table resultTable = report.groupBy($("id"))
//                .select($("id"), $("id").count());
//        sTableEnv.toRetractStream(resultTable,Row.class).print();


        Table table = sTableEnv.sqlQuery("select * from report");
        sTableEnv.toRetractStream(table,Row.class).print();













/*        //准备kafka连接参数
        Properties props = new Properties();
        //props.setProperty("bootstrap.servers", "dcyw1:9092");//集群地址
        //props.setProperty("bootstrap.servers", "39.96.136.60:9092");//集群地址
        //props.setProperty("bootstrap.servers", "39.96.136.7:9092");//集群地址
        //props.setProperty("bootstrap.servers", "39.96.139.70:9092");//集群地址

        props.setProperty("bootstrap.servers", "10.10.41.242:9092");//集群地址
        props.setProperty("bootstrap.servers", "10.10.41.243:9092");//集群地址
        props.setProperty("bootstrap.servers", "10.10.41.251:9092");//集群地址

        props.setProperty("group.id", "flink");//消费者组id
        props.setProperty("auto.offset.reset", "latest");//latest有offset记录从记录位置开始消费,没有记录从最新的/最后的消息开始消费 /earliest有offset记录从记录位置开始消费,没有记录从最早的/最开始的消息开始消费
        //使用连接参数创建FlinkKafkaConsumer/kafkaSource
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<String>("da_trace", new SimpleStringSchema(), props);
        //使用kafkaSource
        DataStream<String> kafkaDS = env.addSource(kafkaSource);*/

        //TODO 2.transformation

        //kafkaDS.print();
        //kafkaDS.addSink(new ckSink());
        env.execute();

    }

    private static class ckSink extends RichSinkFunction<String> {

        private static Connection conn;
        private PreparedStatement preparedStatement;
        String jdbcUrl = "jdbc:clickhouse://10.10.41.242:8123/default";//39.96.136.60:8123,,10.10.41.242:8123,10.10.41.251:8123

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            //加载JDBC驱动
            Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
            //获取数据库连接
            conn = new BalancedClickhouseDataSource(jdbcUrl).getConnection("default", "123456");
            preparedStatement = conn.prepareStatement("INSERT INTO netcat2(size) VALUES (?)");
        }

        @Override
        public void close() throws Exception {
            super.close();
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }

        @Override
        public void invoke(String value, Context context) throws Exception {
            System.out.println(value);

            preparedStatement.setString(1, value);
            preparedStatement.executeUpdate();
            /*try {
                preparedStatement.setString(1, value.getKeywords());
                preparedStatement.setString(2, value.getEventType());
                preparedStatement.setString(3, value.getTraceInTime());
                preparedStatement.setString(4, value.getDeviceId());
                preparedStatement.setString(5, value.getStartFreq());
                preparedStatement.setString(6, value.getStopFreq());
                preparedStatement.setString(7, value.getRbw());
                preparedStatement.setString(8, value.getRefLevel());
                preparedStatement.setString(9, value.getAtt());
                preparedStatement.setString(10, value.getGain());
                preparedStatement.setString(11, value.getPointNum());
                preparedStatement.setString(12, value.getSegmentNum());
                preparedStatement.setString(13, value.getBtraceName());
                preparedStatement.setString(14, value.getEthreshold());
                preparedStatement.setString(15, value.getIndexName());
                preparedStatement.setString(16, value.getCompoundCode());
                preparedStatement.setString(17, value.getData());
                preparedStatement.executeUpdate();

            } catch (Exception e) {
                e.printStackTrace();
            }

        }*/

        }

        @Data
        @NoArgsConstructor
        @AllArgsConstructor
        public static class TraceBean {
            private String keywords;
            private String eventType;
            private String traceInTime;
            private String deviceId;
            private String startFreq;
            private String stopFreq;
            private String rbw;
            private String refLevel;
            private String att;
            private String gain;
            private String pointNum;
            private String segmentNum;
            private String btraceName;
            private String ethreshold;
            private String indexName;
            private String compoundCode;
            private String data;
        }
    }
}
