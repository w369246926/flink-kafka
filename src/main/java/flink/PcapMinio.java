package flink;

import flink.FatMap.PcapFlatMap;
import flink.pojo.Pcap;
import flink.utils.ByteDeSerializer;
import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.errors.MinioException;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class PcapMinio {
    private  static Logger logger = LoggerFactory.getLogger(PcapMinio.class);
    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(args);//main方法中的参数
        ParameterTool cfg = null;
        String topic = "";
        String taskName = "";
        String groupId = "";
        try {
            cfg = ParameterTool.fromPropertiesFile(parameters.get("local_path",null));
            topic = cfg.get("topic")==null?"":cfg.get("topic");
            taskName = cfg.get("taskName")==null?"":cfg.get("taskName");
            groupId = cfg.get("groupId")==null?"":cfg.get("groupId");
        } catch (IOException e) {
            if(e instanceof java.io.FileNotFoundException) {
                logger.info("error: configFilePath:"+cfg + " doesn't exist.");
                return;
            }
        }

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000L);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(3);
        Map properties = new HashMap();
        properties.put("zookeeper.connect", "10.10.41.169:2181,10.10.41.168:2181,10.10.41.241:2181");
        properties.put("bootstrap.servers", "10.10.41.169:6667,10.10.41.168:6667,10.10.41.241:6667");
        properties.put("group.id", "consumerpcap");
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("max.poll.records", "1000");
        properties.put("auto.offset.reset", "latest");//latest  earliest
        properties.put("session.timeout.ms", "30000");
        properties.put("heartbeat.interval.ms", "10000");
        properties.put("max.partition.fetch.bytes", "104857600");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("fetch.max.wait.ms", "500");
        properties.put("fetch.min.bytes", "3655360");
        properties.put("topic", topic);

        ParameterTool parameterTool = ParameterTool.fromMap(properties);
        FlinkKafkaConsumer<byte[]> transction = new FlinkKafkaConsumer(parameterTool.getRequired("topic"),new ByteDeSerializer(), parameterTool.getProperties());
        transction.setStartFromEarliest();
        transction.setStartFromGroupOffsets();
        DataStream<byte[]> transction1 = env.addSource(transction).name("Kafka数据源");

        DataStream<Pcap> dataStream = transction1.keyBy(new KeySelector<byte[], String>() {
            @Override
            public String getKey(byte[] value) throws Exception {
                byte[] data = new byte[8];
                System.arraycopy(value, 0, data, 0, 8);
                ByteArrayInputStream in = new ByteArrayInputStream(data);
                byte[] buffer_4= new byte[4];
                byte[] buffer_2= new byte[2];
                in.read(buffer_4);
                String devIp = bin2Ipv4(buffer_4);
                in.read(buffer_2);
                int devPort =  bin2Port(buffer_2);
                in.read(buffer_2);
                String sequenceNumber =  getMeteDataId(buffer_2);
                String idCus = devIp + devPort + sequenceNumber;  //自定义组合ID
                return idCus;
            }
            public  String bin2Ipv4(byte[] addr) {
                String ip = "";
                for (int i = 0; i < addr.length; i++) {
                    ip += (addr[i] & 0xFF) ;//+ "."
                }
                return ip.substring(0, ip.length() - 1);
            }
            public int bin2Port(byte[] addr) {
                StringBuffer st =new StringBuffer();
                for (int i=0;i<addr.length;i++){
                    String hex = Integer.toHexString(addr[i] & 0xFF);
                    if(hex.length() < 2){
                        st.append(0);
                    }
                    st.append(hex);
                }
                return  Integer.parseInt(String.valueOf(st),16);
            }
            public String getMeteDataId(byte[] b) {
                StringBuilder sBuilder = new StringBuilder();
                for (int i = 0; i < 2; i++) {
                    String zero = "00000000";
                    String binStr = Integer.toBinaryString(b[i] & 0xFF);
                    if (binStr.length() < 8) {
                        binStr = zero.substring(0, 8 - binStr.length()) + binStr;
                    }
                    if (1 == i) {
                        sBuilder.append(binStr.substring(0, 2));
                    } else {
                        sBuilder.append(binStr);
                    }
                }
                return sBuilder.toString();
            }
        })
                .flatMap(new PcapFlatMap())
                .setParallelism(3);
        //sink
        dataStream.addSink(new minioSink());


        env.execute(new String(taskName.getBytes(),"GBK"));
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