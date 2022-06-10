package flink;

import io.minio.*;
import io.minio.errors.MinioException;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import ru.yandex.clickhouse.BalancedClickhouseDataSource;

import java.io.File;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Properties;

public class Tock {
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

//        props.setProperty("bootstrap.servers", "192.168.88.161:9092");//集群地址
//        props.setProperty("bootstrap.servers", "192.168.88.162:9092");//集群地址
//        props.setProperty("bootstrap.servers", "192.168.88.163:9092");//集群地址

//        props.setProperty("bootstrap.servers", "192.168.110.245:9092");//集群地址
//        props.setProperty("bootstrap.servers", "192.168.110.40:9092");//集群地址
//        props.setProperty("bootstrap.servers", "192.168.110.214:9092");//集群地址

        props.setProperty("group.id", "flink2");//消费者组id
        props.setProperty("auto.offset.reset", "latest");//latest有offset记录从记录位置开始消费,没有记录从最新的/最后的消息开始消费 /earliest有offset记录从记录位置开始消费,没有记录从最早的/最开始的消息开始消费
        //使用连接参数创建FlinkKafkaConsumer/kafkaSource
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<String>("metadata2", new SimpleStringSchema(), props);
        //使用kafkaSource
        DataStream<String> kafkaDS = env.addSource(kafkaSource);

        //kafkaDS.print();

        //TODO 2.transformation
        /*DataStream<TraceBean> dataStream = kafkaDS.flatMap(new FlatMapFunction<String, TraceBean>() {

            @Override
            public void flatMap(String value, Collector<TraceBean> out) throws Exception {
                try {
                    //System.out.println(value);
                    if (!value.startsWith("trace") || value.indexOf("[") < 0 || value.indexOf("]") < 0) {
                        //System.out.println("非迹线数据");
                        return;
                    }
                    String[] split = value.split("\\|");
                    if (split.length != 16) {
                        //System.out.println("数组长度不够");
                        return;
                    }
                    //List<Double> jsonArray = JSONArray.parseArray(split[15],Double.class);
                    TraceBean traceBean = new TraceBean();
                    traceBean.setKeywords(split[2] + "-" + split[3] + "-" + split[4]);
                    traceBean.setEventType(split[0]);
                    traceBean.setTraceInTime(split[1]);
                    traceBean.setDeviceId(split[2]);
                    traceBean.setStartFreq(split[3]);
                    traceBean.setStopFreq(split[4]);
                    traceBean.setRbw(split[5]);
                    traceBean.setRefLevel(split[6]);
                    traceBean.setAtt(split[7]);
                    traceBean.setGain(split[8]);
                    traceBean.setPointNum(split[9]);
                    traceBean.setSegmentNum(split[10]);
                    traceBean.setBtraceName(split[11]);
                    traceBean.setEthreshold(split[12]);
                    traceBean.setIndexName(split[13]);
                    traceBean.setCompoundCode(split[14]);
                    traceBean.setData(split[15]);
                    out.collect(traceBean);
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        });*/

        kafkaDS.print();
        //kafkaDS.writeAsText("s3://10.10.41.251:9090/flinkstreamfilesink/data");
        //kafkaDS.addSink(new ckSink());
        //kafkaDS.addSink(new minioSink());
        env.execute();

    }

    private static class ckSink extends RichSinkFunction<String> {

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
