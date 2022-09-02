package flink;

import cn.hutool.core.util.IdUtil;
import com.alibaba.fastjson.JSONObject;
import flink.dawarning.source.MyKafkaUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import ru.yandex.clickhouse.BalancedClickhouseDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class Tockahwarning {
    public static void main(String[] args) throws Exception {
        //TODO 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        //env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        String topic = "ah_warning";
        String groupid = "flink";
        DataStreamSource<String> stringDataStreamSource = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupid));


        //TODO 1.source
        //准备kafka连接参数
        /*Properties props = new Properties();
        props.setProperty("bootstrap.servers", "10.10.42.241:9092");//集群地址
        props.setProperty("bootstrap.servers", "10.10.42.242:9092");//集群地址
        props.setProperty("bootstrap.servers", "10.10.42.243:9092");//集群地址
        props.setProperty("group.id", "flink0902");//消费者组id
        //props.setProperty("auto.offset.reset", "latest");//latest有offset记录从记录位置开始消费,没有记录从最新的/最后的消息开始消费 /earliest有offset记录从记录位置开始消费,没有记录从最早的/最开始的消息开始消费
        //使用连接参数创建FlinkKafkaConsumer/kafkaSource
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<String>("ah_warning", new SimpleStringSchema(), props);
*/        //使用kafkaSource
//        DataStream<String> kafkaDS = env.addSource(kafkaSource).setParallelism(3);;
        //TODO 2.transformation
        DataStream<JSONObject> dataStream = stringDataStreamSource.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                //有大括号[json]
                if (s.substring(0, 1).equals("[")) {
                    //截取需求部分
                    String substring = s.substring(1, (s.length()-2));
                    //将不规则字符更换为规则字符
                    String replace = substring.replace("[", "\"").replace("]","\"");
                    System.out.println("yes[]----" + replace);
                    JSONObject jsonObject = JSONObject.parseObject(replace);
                    collector.collect(jsonObject);
                } else {
                    //无大括号json
                    //System.out.println("no[]----" + s);
                    //将不规则字符更换为规则字符
                    //String replace = s.replace("[", "\"").replace("]","\"");
                    JSONObject jsonObject = JSONObject.parseObject(s);
                    jsonObject.put("uuid", IdUtil.simpleUUID());
                    long date = System.currentTimeMillis();
                    jsonObject.put("date", date);
                    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//可以方便地修改日期格式
                    String currenttime = dateFormat.format(date);
                    jsonObject.put("currenttime", currenttime);
                    collector.collect(jsonObject);
                }
            }
        });


        //TODO 3.sink


        //dataStream.print();
        dataStream.addSink(new ckSink());


        env.execute("安恒告警日志数据");

    }

    private static class ckSink extends RichSinkFunction<JSONObject> {
        String key = "date,uuid,sendType,productVendorName,deviceSendProductName," +
                "deviceProductType,deviceAssetSubType,customerId,deviceId,eventCount," +
                "securityEyeLogType,productId,companyId,hostId,groupId,projectName," +
                "sendHostName,deviceName,sendHostAddress,deviceAddress,deviceMac," +
                "machineCode,hostType,transProtocol,protocolType,appProtocol,sendLogId," +
                "logSessionId,srcAddress,srcPort,srcMacAddress,destAddress,destPort," +
                "destMacAddress,vlanID,srcGeoCountry,srcGeoRegion,srcGeoCity,srcGeoUnit," +
                "srcGeoAddress,srcGeoLatitude,srcGeoLongitude,srcGeoPoint,destGeoCountry," +
                "destGeoRegion,destGeoCity,destGeoUnit,destGeoAddress,destGeoLatitude," +
                "destGeoLongitude,destGeoPoint,srcSecurityZone,destSecurityZone,direction," +
                "severity,name,deviceCat,catObject,catBehavior,catTechnique,catOutcome," +
                "catSignificance,eventId,startTime,endTime,collectorReceiptTime," +
                "deviceReceiptTime,srcTransAddress,ruleId,ruleName,ruleType,ruleLevel," +
                "alarmMessage,payload,alarmDesc,ruleLink,fileName,fileMd5,attackFileType," +
                "requestDomain,responseCode,attackStage,attackDirection,attackStatus," +
                "pcapRecord,requestMethod,requestUrlQuery,requestHeader,requestBody," +
                "responseHeader,responseMsg,srcUserName,passwd,destHostName,IoC,sclass," +
                "IoCLevel,confidence,IoCType,mclass,fileType,fileSize,sha256,virusType," +
                "virusName,sandboxReportId,message,alertType,currenttime" ;
        String sql= "";
        private Statement stmt;
        private  Connection conn;
        private PreparedStatement preparedStatement;
        String jdbcUrl = "jdbc:clickhouse://10.10.42.241:8123/default";//39.96.136.60:8123,,10.10.41.242:8123,10.10.41.251:8123
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
            if (conn != null) {
                conn.close();
            }
        }
        @Override
        public void invoke(JSONObject value, Context context) throws Exception {
            try{
                //获取所有告警的JSON
                String[] splitkey = key.split(",");

                //StringBuilder columns = new StringBuilder();
                StringBuilder values = new StringBuilder();
                values.append("'");

                for (int i = 0; i < splitkey.length; i++) {

                    if (i == splitkey.length-1){
                        //System.out.println(splitkey.length);
                        values.append(value.getOrDefault(splitkey[i], "-1")).append("'");
                    }else {
                        values.append(value.getOrDefault(splitkey[i], "-1")).append("','");
                    }
                }

                String sqlvalue = values.toString();
                sql = "INSERT INTO default.bus_ahwarning ( "+ key+" ) VALUES ( "+sqlvalue +" )";
                System.out.println("A表");
                stmt.executeQuery(sql);

            }catch (Exception e){
                System.out.println(e);
            }
        }
    }

}
