package flink.test;

import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.errors.MinioException;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import ru.yandex.clickhouse.BalancedClickhouseDataSource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Properties;

public class Tocktest1 {
    public static <Tuple58> void main(String[] args) throws Exception {
        //TODO 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //TODO 1.source
        DataStreamSource<String> lines = env.readTextFile("./data/input/bus_twarning_local_202205171517.csv").setParallelism(1);
        //bus_twarning_local_202205171517.csv


        //lines.print();

        //TODO 2.transformation
//        lines.map(new MapFunction<String, Tuple58<Integer,String,Integer,Integer,String,Integer,String,String,Integer,Integer,Integer,Integer,Integer,Integer,String,String,String,Integer,Integer,Integer,Integer,Integer,Integer,String,Integer,String,Integer,Integer,String,Integer,String,Integer,String,String,String,String,String,String,String,String,String,Integer,String,String,Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer,String,String,String,String,String>>() {
//        })
        DataStream<TraceBean> out = lines.flatMap(new FlatMapFunction<String, TraceBean>() {
            @Override
            public void flatMap(String s, Collector<TraceBean> collector) throws Exception {
                String[] split = s.split(",");
                TraceBean traceBean = new TraceBean();
                traceBean.setId(split[0]);
                traceBean.setMessageProtocolVersion(split[1]);
                traceBean.setMessageDeviceTypeId(split[2]);
                traceBean.setMessageProductId(split[3]);
                traceBean.setMessageDeviceDescribe(split[4]);
                traceBean.setMessageEmbeddedSoftwareVersion(split[5]);
                traceBean.setMessageChipVersion(split[6]);
                traceBean.setMessageDeviceSerialId(split[7]);
                traceBean.setMessagePackageId(split[8]);
                traceBean.setMessageLoadLength(split[9]);
                traceBean.setMessageNumber(split[10]);
                traceBean.setMessageSplitSerialId(split[11]);
                traceBean.setVerifyCode(split[12]);
                traceBean.setReserved(split[13]);
                traceBean.setDestMac(split[14]);
                traceBean.setSrcIp(split[15]);
                traceBean.setDestIp(split[16]);
                traceBean.setSrcPort(split[17]);
                traceBean.setDestPort(split[18]);
                traceBean.setProtocolType(split[19]);
                traceBean.setByteNumber(split[20]);
                traceBean.setPackageNumber(split[21]);
                traceBean.setVerifyMessageBodyType(split[22]);
                traceBean.setVerifyTypeId(split[23]);
                traceBean.setLoadLength(split[24]);
                traceBean.setVerifyFunctionModuleCode(split[25]);
                traceBean.setTempVerifyCode(split[26]);
                traceBean.setMessageSplit(split[27]);
                traceBean.setSrcMac(split[28]);
                traceBean.setVlan(split[29]);
                traceBean.setTagDescribe(split[30]);
                traceBean.setDate(split[31]);
                traceBean.setUuid(split[32]);
                traceBean.setEvent_level(split[33]);
                traceBean.setDisposal(split[34]);
                traceBean.setDisposal_instructions(split[35]);
                traceBean.setDownPackageNumber(split[36]);
                traceBean.setUpByteNumber(split[37]);
                traceBean.setChangeIp(split[38]);
                traceBean.setChangePort(split[39]);
                traceBean.setUpPackageNumber(split[40]);
                traceBean.setCurrentState(split[41]);
                traceBean.setDownByteNumber(split[42]);
                traceBean.setSize(split[43]);
                traceBean.setStatus(split[44]);
                traceBean.setMessageBodyType(split[45]);
                traceBean.setMessageBodyId(split[46]);
                traceBean.setMessageBodyLength(split[47]);
                traceBean.setSendSourceModuleId(split[48]);
                traceBean.setFunctionModuleCode(split[49]);
                traceBean.setMessageInputPhysicsPort(split[50]);
                traceBean.setTimeStamp(split[51]);
                traceBean.setTypeId(split[52]);
                traceBean.setMessageSplitdestIp(split[53]);
                traceBean.setClassification(split[54]);
                traceBean.setTriggerStrategy(split[55]);
                traceBean.setStrategyType(split[56]);
                traceBean.setPid(split[57]);
                collector.collect(traceBean);
            }
        });


        //out.print();


        out.addSink(new ckSink());

        env.execute();

    }

    private static class ckSink extends RichSinkFunction<TraceBean> {

        private static Connection conn;
        private Statement stmt;
        private PreparedStatement preparedStatement;
        String jdbcUrl = "jdbc:clickhouse://39.96.136.7:8123/default";//39.96.136.60:8123,,10.10.41.242:8123,10.10.41.251:8123

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            //加载JDBC驱动
            Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
            //获取数据库连接
            //conn = new BalancedClickhouseDataSource(jdbcUrl).getConnection("default", "123456");
            conn = DriverManager.getConnection("jdbc:clickhouse://39.96.136.7:8123/default","default","123456");


            //stmt = conn.createStatement();
            preparedStatement = conn.prepareStatement("INSERT INTO bus_twarning_local(id , messageProtocolVersion , messageDeviceTypeId , messageProductId , messageDeviceDescribe , messageEmbeddedSoftwareVersion , messageChipVersion , messageDeviceSerialId , messagePackageId , messageLoadLength , messageNumber , messageSplitSerialId , verifyCode , reserved , destMac , srcIp , destIp , srcPort , destPort , protocolType , byteNumber , packageNumber , verifyMessageBodyType , verifyTypeId , loadLength , verifyFunctionModuleCode , tempVerifyCode , messageSplit , srcMac , vlan , tagDescribe , date , uuid , event_level , disposal , disposal_instructions , downPackageNumber , upByteNumber , changeIp , changePort , upPackageNumber , currentState , downByteNumber , size , status , messageBodyType , messageBodyId , messageBodyLength , sendSourceModuleId , functionModuleCode , messageInputPhysicsPort , timeStamp , typeId , messageSplitdestIp , classification , triggerStrategy , strategyType , pid ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
        }

        @Override
        public void close() throws Exception {
            super.close();
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }

        @Override
        public void invoke(TraceBean value, Context context) throws Exception {

            try {
                preparedStatement.setString(1,value.getId( ));
                preparedStatement.setString(2,value.getMessageProtocolVersion( ));
                preparedStatement.setString(3,value.getMessageDeviceTypeId( ));
                preparedStatement.setString(4,value.getMessageProductId( ));
                preparedStatement.setString(5,value.getMessageDeviceDescribe( ));
                preparedStatement.setString(6,value.getMessageEmbeddedSoftwareVersion( ));
                preparedStatement.setString(7,value.getMessageChipVersion( ));
                preparedStatement.setString(8,value.getMessageDeviceSerialId( ));
                preparedStatement.setString(9,value.getMessagePackageId( ));
                preparedStatement.setString(10,value.getMessageLoadLength( ));
                preparedStatement.setString(11,value.getMessageNumber( ));
                preparedStatement.setString(12,value.getMessageSplitSerialId( ));
                preparedStatement.setString(13,value.getVerifyCode( ));
                preparedStatement.setString(14,value.getReserved( ));
                preparedStatement.setString(15,value.getDestMac( ));
                preparedStatement.setString(16,value.getSrcIp( ));
                preparedStatement.setString(17,value.getDestIp( ));
                preparedStatement.setString(18,value.getSrcPort( ));
                preparedStatement.setString(19,value.getDestPort( ));
                preparedStatement.setString(20,value.getProtocolType( ));
                preparedStatement.setString(21,value.getByteNumber( ));
                preparedStatement.setString(22,value.getPackageNumber( ));
                preparedStatement.setString(23,value.getVerifyMessageBodyType( ));
                preparedStatement.setString(24,value.getVerifyTypeId( ));
                preparedStatement.setString(25,value.getLoadLength( ));
                preparedStatement.setString(26,value.getVerifyFunctionModuleCode( ));
                preparedStatement.setString(27,value.getTempVerifyCode( ));
                preparedStatement.setString(28,value.getMessageSplit( ));
                preparedStatement.setString(29,value.getSrcMac( ));
                preparedStatement.setString(30,value.getVlan( ));
                preparedStatement.setString(31,value.getTagDescribe( ));
                preparedStatement.setString(32,value.getDate( ));
                preparedStatement.setString(33,value.getUuid( ));
                preparedStatement.setString(34,value.getEvent_level( ));
                preparedStatement.setString(35,value.getDisposal( ));
                preparedStatement.setString(36,value.getDisposal_instructions( ));
                preparedStatement.setString(37,value.getDownPackageNumber( ));
                preparedStatement.setString(38,value.getUpByteNumber( ));
                preparedStatement.setString(39,value.getChangeIp( ));
                preparedStatement.setString(40,value.getChangePort( ));
                preparedStatement.setString(41,value.getUpPackageNumber( ));
                preparedStatement.setString(42,value.getCurrentState( ));
                preparedStatement.setString(43,value.getDownByteNumber( ));
                preparedStatement.setString(44,value.getSize( ));
                preparedStatement.setString(45,value.getStatus( ));
                preparedStatement.setString(46,value.getMessageBodyType( ));
                preparedStatement.setString(47,value.getMessageBodyId( ));
                preparedStatement.setString(48,value.getMessageBodyLength( ));
                preparedStatement.setString(49,value.getSendSourceModuleId( ));
                preparedStatement.setString(50,value.getFunctionModuleCode( ));
                preparedStatement.setString(51,value.getMessageInputPhysicsPort( ));
                preparedStatement.setString(52,value.getTimeStamp( ));
                preparedStatement.setString(53,value.getTypeId( ));
                preparedStatement.setString(54,value.getMessageSplitdestIp( ));
                preparedStatement.setString(55,value.getClassification( ));
                preparedStatement.setString(56,value.getTriggerStrategy( ));
                preparedStatement.setString(57,value.getStrategyType( ));
                preparedStatement.setString(58,value.getPid( ));
                preparedStatement.executeUpdate();

            } catch (Exception e) {
                e.printStackTrace();
            }

        }

        }

        @Data
        @NoArgsConstructor
        @AllArgsConstructor
        public static class TraceBean {
            String id;
            String messageProtocolVersion;
            String messageDeviceTypeId;
            String messageProductId;
            String messageDeviceDescribe;
            String messageEmbeddedSoftwareVersion;
            String messageChipVersion;
            String messageDeviceSerialId;
            String messagePackageId;
            String messageLoadLength;
            String messageNumber;
            String messageSplitSerialId;
            String verifyCode;
            String reserved;
            String destMac;
            String srcIp;
            String destIp;
            String srcPort;
            String destPort;
            String protocolType;
            String byteNumber;
            String packageNumber;
            String verifyMessageBodyType;
            String verifyTypeId;
            String loadLength;
            String verifyFunctionModuleCode;
            String tempVerifyCode;
            String messageSplit;
            String srcMac;
            String vlan;
            String tagDescribe;
            String date;
            String uuid;
            String event_level;
            String disposal;
            String disposal_instructions;
            String downPackageNumber;
            String upByteNumber;
            String changeIp;
            String changePort;
            String upPackageNumber;
            String currentState;
            String downByteNumber;
            String size;
            String status;
            String messageBodyType;
            String messageBodyId;
            String messageBodyLength;
            String sendSourceModuleId;
            String functionModuleCode;
            String messageInputPhysicsPort;
            String timeStamp;
            String typeId;
            String messageSplitdestIp;
            String classification;
            String triggerStrategy;
            String strategyType;
            String pid;
        }


    private static class minioSink implements SinkFunction {
        @Override
        public void invoke(Object value, Context context) throws Exception {

            //InputStream in = value.getInputStream();
            try {
// Create a minioClient with the MinIO server playground, its access keyand secret key.
                MinioClient minioClient =
                        MinioClient.builder()
                                .endpoint("http://10.10.41.251:9090")
                                .credentials("adminminio", "admin123456")
                                .build();
// 创建bucket
                String bucketName = "flinkstreamfilesink";
                boolean exists =
                        minioClient.bucketExists(BucketExistsArgs.builder().bucket(bucketName).build());
                if (!exists) {
// 不存在，创建bucket
                    minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
                }
// 上传文件
                long l = System.currentTimeMillis();
                String name = String.valueOf(l);
//                minioClient.putObject(PutObjectArgs.builder().bucket(bucketName).object(name)
//                        .stream(stream, -1, 10485760).build());
                System.out.println("上传文件成功");
            } catch (MinioException e) {
                System.out.println("Error occurred: " + e);
                System.out.println("HTTP trace: " + e.httpTrace());
            }

        }
    }

}
