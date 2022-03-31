package flink.test;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import flink.bean.Basic;
import flink.bean.Report;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import ru.yandex.clickhouse.BalancedClickhouseDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class Tockjson {
    public static void main(String[] args) throws Exception {
        //TODO 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //TODO 1.source
        //准备kafka连接参数
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
        DataStream<String> kafkaDS = env.addSource(kafkaSource);

        kafkaDS.print("接受时");

        //TODO 2.transformation
        DataStream<Report> dataStream = kafkaDS.flatMap(new FlatMapFunction<String,Report>() {

            @Override
            public void flatMap(String value, Collector<Report> out) throws Exception {
                try {
                    //JSONObject jsonObject = JSONObject.parseObject(value, JSONObject.class);
                    Report report = JSONObject.parseObject(value, Report.class);
//                    System.out.println("接受前");
                    System.out.println("json"+report);
/*//                    System.out.println("接收后");
                    Report report1 = new Report();
                    //String messageSplit = report.getMessageSplit();
                    //messageProtocolVersion;
                    report1.setMessageProtocolVersion(report.getMessageProtocolVersion());
                    //messageDeviceTypeId;
                    report1.setMessageDeviceTypeId(report.getMessageDeviceTypeId());
//                    messageProductId;
                    report1.setMessageProductId(report.getMessagePackageId());
//                    messageDeviceDescribe;
                    report1.setMessageDeviceDescribe(report.getMessageDeviceDescribe());
//                    messageEmbeddedSoftwareVersion;
                    report1.setMessageEmbeddedSoftwareVersion(report.getMessageEmbeddedSoftwareVersion());
//                    messageChipVersion;
                    report1.setMessageChipVersion(report.getMessageChipVersion());
//                    messageDeviceSerialId;
                    report1.setMessageDeviceSerialId(report.getMessageDeviceSerialId());
//                    messagePackageId;
                    report1.setMessagePackageId(report.getMessagePackageId());
//                    messageLoadLength;
                    report1.setMessageLoadLength(report.getMessageLoadLength());
//                    messageNumber;
                    report1.setMessageNumber(report.getMessageNumber());
//                    messageSplitSerialId;
                    report1.setMessageSplitSerialId(report.getMessageSplitSerialId());
//                    verifyCode;
                    report1.setVerifyCode(report.getVerifyCode());
//                    reserved;
                    report1.setReserved(report.getReserved());
//                    basic;
                    report1.setBasic(report.getBasic());
//                    tempVerifyCode;
                    report1.setTempVerifyCode(report.getTempVerifyCode());
//                    messageSplit
                    report1.setMessageSplit(report.getMessageSplit());*/
                    out.collect(report);
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        });

        //dataStream.print();
        dataStream.addSink(new ckSink());
        env.execute();

    }

    private static class ckSink extends RichSinkFunction<Report> {

        String reportindex = "";
        private Statement stmt=null;
        private static Connection conn;
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
            //preparedStatement = conn.prepareStatement("INSERT INTO report(size) VALUES (?)");
        }

        @Override
        public void close() throws Exception {
            super.close();
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }

        @Override
        public void invoke(Report value, Context context) throws Exception {
            StringBuilder columns = new StringBuilder("a,b,c,d,e");
            StringBuilder values = new StringBuilder();
            System.out.println("刚接到"+value);
            try {
                values.append(value.getMessageProtocolVersion()).append("',");
                values.append(value.getMessageDeviceTypeId());
                values.append(value.getMessageProductId());
                values.append(value.getMessageDeviceDescribe());
                values.append(value.getMessageEmbeddedSoftwareVersion());
                values.append(value.getMessageDeviceSerialId());
                values.append(value.getMessageChipVersion());
                values.append(value.getMessageDeviceSerialId());
                values.append(value.getMessagePackageId());
                values.append(value.getMessageLoadLength());
                values.append(value.getMessageNumber());
                values.append(value.getMessageSplitSerialId());
                values.append(value.getVerifyCode());
                values.append(value.getReserved());
                //取出List
                List objects = value.getBasicMessageBasicList();
                //JSONArray objects = JSONObject.parseArray(basic);
                //System.out.println(objects.size());

                for (int i = 0; i < objects.size(); i++) {
                    //JSONObject jsonObject = objects.getJSONObject(i);
                    Object o = objects.get(i);
                    //String skey = columns.append(jsonObject).append(",").toString();
                    String svalue = values.append(o).append(",").toString();
                    //System.out.println("执行前");
                    //System.out.println("hou" + skey + "and" + svalue);
                    System.out.println("执行后");

                    //101
                    //method a
                    //String sql="";
                    //stmt.addBatch(sql);
                }
            }catch (Exception e){

            }
            //stmt.executeBatch();
            //stmt.clearBatch();

//            preparedStatement.setString(1, value.getMessageProtocolVersion());
//            preparedStatement.setString(2, value.getMessageDeviceTypeId());
//            preparedStatement.setString(3, value.getMessageProductId());
//            preparedStatement.setString(4, value.getMessageDeviceDescribe());
//            preparedStatement.setString(5, value.getMessageEmbeddedSoftwareVersion());
//            preparedStatement.setString(6, value.getMessageDeviceSerialId());
//            preparedStatement.setString(7, value.getMessageChipVersion());
//            preparedStatement.setString(8, value.getMessageDeviceSerialId());
//            preparedStatement.setString(9, value.getMessagePackageId());
//            preparedStatement.setString(10, value.getMessageLoadLength());
//            preparedStatement.setString(11, value.getMessageNumber());
//            preparedStatement.setString(12, value.getMessageSplitSerialId());
//            preparedStatement.setString(13, value.getVerifyCode());
//            preparedStatement.setString(14, value.getReserved());
//            reportindex = value.getBasic().getVerifyTypeId();
//            if (reportindex.equals("101")) {
//                preparedStatement.setString(15, value.getBasic().getDestMac());
//                preparedStatement.setString(16, value.getBasic().getDestIp());
//                preparedStatement.setString(17, value.getBasic().getSrcPort());
//                preparedStatement.setString(18, value.getBasic().getDestPort());
//                preparedStatement.setString(19, value.getBasic().getProtocolType());
//                preparedStatement.setString(20, value.getBasic().getByteNumber());
//                preparedStatement.setString(21, value.getBasic().getPackageNumber());
//                preparedStatement.setString(22, value.getBasic().getVerifyMessageBodyType());
//                preparedStatement.setString(23, value.getBasic().getVerifyTypeId());
//                preparedStatement.setString(24, value.getBasic().getLoadLength());
//                preparedStatement.setString(25, value.getBasic().getVerifyFunctionModuleCode());
//            }else if (reportindex.equals("102")){


//            preparedStatement.setString(26, value.getTempVerifyCode());
//            preparedStatement.setString(27, value.getMessageSplit());
//            preparedStatement.executeUpdate();






//            Iterator iter = value.entrySet().iterator();
//            while (iter.hasNext()) {
//                Map.Entry entry = (Map.Entry) iter.next();
//                //System.out.println(entry.getKey().toString());
//                //System.out.println(entry.getValue().toString());
//                Object keys = entry.getKey();
//                Object values = entry.getValue();
//                System.out.println(keys);
//                System.out.println(values);
//            }

            //preparedStatement.setString(1, value);
            //preparedStatement.executeUpdate();
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


    }


}
