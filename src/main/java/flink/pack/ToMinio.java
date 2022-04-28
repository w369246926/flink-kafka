//package flink.pack;
//
//import lombok.AllArgsConstructor;
//import lombok.Data;
//import lombok.NoArgsConstructor;
//import org.apache.commons.beanutils.BeanUtils;
//import org.apache.commons.lang3.ArrayUtils;
//import org.apache.flink.api.common.functions.RichFlatMapFunction;
//import org.apache.flink.api.common.serialization.DeserializationSchema;
//import org.apache.flink.api.common.serialization.SimpleStringSchema;
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.api.java.functions.KeySelector;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.datastream.KeyedStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
//import org.apache.flink.util.Collector;
//import org.apache.kafka.clients.consumer.ConsumerRecord;
//import ru.yandex.clickhouse.BalancedClickhouseDataSource;
//
//import java.io.ByteArrayInputStream;
//import java.io.IOException;
//import java.sql.Connection;
//import java.sql.PreparedStatement;
//import java.util.Map;
//import java.util.Properties;
//
//public class ToMinio {
//    public static <Pcap> void main(String[] args) throws Exception {
//        //TODO 0.env
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        //env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
//
//        //TODO 1.source
//        //准备kafka连接参数
//        Properties props = new Properties();
//        props.setProperty("bootstrap.servers", "10.10.41.242:9092");//集群地址
//        props.setProperty("bootstrap.servers", "10.10.41.243:9092");//集群地址
//        props.setProperty("bootstrap.servers", "10.10.41.251:9092");//集群地址
//
//        props.setProperty("group.id", "flink");//消费者组id
//        props.setProperty("auto.offset.reset", "latest");//latest有offset记录从记录位置开始消费,没有记录从最新的/最后的消息开始消费 /earliest有offset记录从记录位置开始消费,没有记录从最早的/最开始的消息开始消费
//        //使用连接参数创建FlinkKafkaConsumer/kafkaSource
//        FlinkKafkaConsumer<byte[]> kafkaSource = new FlinkKafkaConsumer<byte[]>("packdata", (DeserializationSchema<byte[]>) new ByteDeSerializer(), props);
//        kafkaSource.setStartFromEarliest();
//        kafkaSource.setStartFromGroupOffsets();
//        //使用kafkaSource
//        DataStream<byte[]> kafkaDS = env.addSource(kafkaSource);
//
//        //kafkaDS.print();
//
//        //TODO 2.transformation
//        KeyedStream<byte[], String> stringKeyedStream = kafkaDS.keyBy(new KeySelector<byte[], String>() {
//            @Override
//            public String getKey(byte[] value) throws Exception {
//                byte[] data = new byte[8];
//                System.arraycopy(value, 0, data, 0, 8);
//                ByteArrayInputStream in = new ByteArrayInputStream(data);
//                byte[] buffer_4 = new byte[4];
//                byte[] buffer_2 = new byte[2];
//                in.read(buffer_4);
//                String devIp = bin2Ipv4(buffer_4);
//                in.read(buffer_2);
//                int devPort = bin2Port(buffer_2);
//                in.read(buffer_2);
//                String sequenceNumber = getMeteDataId(buffer_2);
//                String idCus = devIp + devPort + sequenceNumber;  //自定义组合ID
//                return idCus;
//            }
//
//            public String bin2Ipv4(byte[] addr) {
//                String ip = "";
//                for (int i = 0; i < addr.length; i++) {
//                    ip += (addr[i] & 0xFF);//+ "."
//                }
//                return ip.substring(0, ip.length() - 1);
//            }
//
//            public int bin2Port(byte[] addr) {
//                StringBuffer st = new StringBuffer();
//                for (int i = 0; i < addr.length; i++) {
//                    String hex = Integer.toHexString(addr[i] & 0xFF);
//                    if (hex.length() < 2) {
//                        st.append(0);
//                    }
//                    st.append(hex);
//                }
//                return Integer.parseInt(String.valueOf(st), 16);
//            }
//
//            public String getMeteDataId(byte[] b) {
//                StringBuilder sBuilder = new StringBuilder();
//                for (int i = 0; i < 2; i++) {
//                    String zero = "00000000";
//                    String binStr = Integer.toBinaryString(b[i] & 0xFF);
//                    if (binStr.length() < 8) {
//                        binStr = zero.substring(0, 8 - binStr.length()) + binStr;
//                    }
//                    if (1 == i) {
//                        sBuilder.append(binStr.substring(0, 2));
//                    } else {
//                        sBuilder.append(binStr);
//                    }
//                }
//                return sBuilder.toString();
//            }
//        });
//        stringKeyedStream.flatMap(new RichFlatMapFunction<byte[], Pcap>() {
//            @Override
//            public void flatMap(byte[] bytes, Collector<Pcap> collector) throws Exception {
//                ByteArrayInputStream in = new ByteArrayInputStream(bytes);
//                byte[] buffer_4= new byte[4];
////        byte[] buffer_2= new byte[2];
//                //解析发数据的Ip
////        in.read(buffer_4);
////        String devIp = "";
//                //解析发数据的 port
////        in.read(buffer_2);
////        int devPort = 0;
//                //解析metaData
//                in.read(buffer_4);
//                //0-4字节
//                String meteDataId = getMeteDataId(buffer_4);
//                int sequenceNumber = Integer.parseInt(meteDataId.substring(0,10),2);
//                int secondFrag = Integer.parseInt(meteDataId.substring(10,11));
//                int firstFrag = Integer.parseInt(meteDataId.substring(11,12));
//                int meteType =  Integer.parseInt(meteDataId.substring(12,14),2);
//                int flowMatch =Integer.parseInt( meteDataId.substring(14,15));
//                int flowId =Integer.parseInt( meteDataId.substring(15,29),2);
//                int sourcePort = Integer.parseInt(meteDataId.substring(29,32),2);
//                in.read(buffer_4);
//                //4-8字节
////        解析
////        Package_index  24
////        SN_device 2
////        SN_produc 4
////        SN_batch 2
//                String meteDataId1 = getMeteDataId(buffer_4);
//                int Package_index  = Integer.parseInt(meteDataId1.substring(0,24),2);
//                String SN_device  = meteDataId1.substring(24,26);
//                String SN_produc  = meteDataId1  .substring(26,30);
//                String SN_batch  = meteDataId1.substring(30,32);
//                if(SN_produc.equals("0000")){
//                    SN_produc="7100";
//                }else if (SN_produc.equals("0001")){
//                    SN_produc="7020";
//                }
//                in.read(buffer_4);
//                //8-12字节
//                String SNNumTmp = new String(buffer_4,"utf-8");
////        logger.info("SNNumTmp"+SNNumTmp);
//                // 生成最终的设备序列号  +"-"+
//                String SNNum ="JAJX"+SN_device+SN_produc+SN_batch+SNNumTmp;
//                logger.info("SNNum"+SNNum);
//                String idCus = SNNum + sequenceNumber;  //自定义组合ID
//                Pcap pcap1 = new Pcap(SNNum, meteDataId, sequenceNumber, secondFrag, firstFrag, meteType, flowMatch, flowId, sourcePort);
//                if(firstFrag == 0 && secondFrag == 0 ){// 无分片  直接存储
//                    Map<String, Object> pcapInfo = getPcapInfo(in);
//                    if(pcapInfo==null ){
////                logger.info("pcapInfo==null  数据格式错误 "+(pcapInfo==null));
//                        return ;
//                    }
//                    BeanUtils.populate(pcap1,pcapInfo);
////            logger.info("pcapInfo 0"+pcapInfo.toString());
//                    out.collect(pcap1);
//                }else if(firstFrag < secondFrag ){// 第二分片 直接存储
//                    //判断有没有第一个分片
//                    Pcap pcap = mapState.get(idCus+ "01");
//                    byte[] buffer_all = new byte[in.available()];
//                    if(pcap == null ){//没有分片  进行存储
//                        pcap1.setPacketData(buffer_all);
//                        mapState.put(idCus +  "10",pcap1);
//                        logger.info("pcapInfo 1");
//                        return;
//                    }
//                    mapState.remove(idCus +  "01");
//                    pcap1.setFirstFrag(1);// SecondFrag 和 FirstFrag均为1 表示合并包完毕
//                    byte[] data = pcap.getPacketData();
//                    in.read(buffer_all);  //  读取第2分片中数据
//                    byte[] data1 = ArrayUtils.addAll(data, buffer_all);
//                    Map<String, Object> pcapInfo = getPcapInfo(new ByteArrayInputStream(data1));
//                    if(pcapInfo==null ){
////                logger.info("pcapInfo==null" + (pcapInfo==null));
//                        return ;
//                    }
//                    BeanUtils.populate(pcap1,pcapInfo);
//                    logger.info("pcapInfo 2" + pcapInfo.toString());
//                    out.collect(pcap1);
//                }else {//第一分片
//                    //判断有没有第二个分片
//                    Pcap pcap = mapState.get(idCus+ "10");
//                    byte[] buffer_all = new byte[in.available()];
//                    if(pcap == null ){//  没有分片 进行存储
//                        pcap1.setPacketData(buffer_all);
//                        mapState.put(idCus+ "01",pcap1);
//                        logger.info("pcapInfo 1");
//                        return ;
//                    }
//                    mapState.remove(idCus+"10");
//                    //分片重组
//                    byte[] data = pcap.getPacketData();
//                    pcap1.setSecondFrag(1);// SecondFrag 和 FirstFrag均为1 表示合并包完毕
//                    in.read(buffer_all);  //  读取第2分片中数据
//                    byte[] data1 = ArrayUtils.addAll(buffer_all, data);
//                    Map<String, Object> pcapInfo = getPcapInfo(new ByteArrayInputStream(data1));
//                    if(pcapInfo==null ){
////                logger.info("pcapInfo==null"+(pcapInfo==null));
//                        return ;
//                    }
//                    BeanUtils.populate(pcap1,pcapInfo);
//                    logger.info("pcapInfo 1"+pcapInfo.toString());
//                    out.collect(pcap1);
//            }
//        })
////                .flatMap(new )
////                .setParallelism(3);
//
//
//        //TODO 3.sink
//        kafkaDS.print();
//        kafkaDS.addSink(new ckSink());
//        //TODO 4.execute
//        env.execute("");
//
//    }
//
//    private static class ckSink extends RichSinkFunction<String> {
//
//        private static Connection conn;
//        private PreparedStatement preparedStatement;
//        String jdbcUrl = "jdbc:clickhouse://10.10.41.251:8123/default";//39.96.136.60:8123,,10.10.41.242:8123,10.10.41.251:8123
//
//        @Override
//        public void open(Configuration parameters) throws Exception {
//            super.open(parameters);
//            //加载JDBC驱动
//            Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
//            //获取数据库连接
//            conn = new BalancedClickhouseDataSource(jdbcUrl).getConnection("default", "123456");
//            preparedStatement = conn.prepareStatement("INSERT INTO netcat2(size) VALUES (?)");
//        }
//
//        @Override
//        public void close() throws Exception {
//            super.close();
//            if (preparedStatement != null) {
//                preparedStatement.close();
//            }
//        }
//
//        @Override
//        public void invoke(String value, Context context) throws Exception {
//            System.out.println(value);
//
//            preparedStatement.setString(1, value);
//            preparedStatement.executeUpdate();
//            /*try {
//                preparedStatement.setString(1, value.getKeywords());
//                preparedStatement.setString(2, value.getEventType());
//                preparedStatement.setString(3, value.getTraceInTime());
//                preparedStatement.setString(4, value.getDeviceId());
//                preparedStatement.setString(5, value.getStartFreq());
//                preparedStatement.setString(6, value.getStopFreq());
//                preparedStatement.setString(7, value.getRbw());
//                preparedStatement.setString(8, value.getRefLevel());
//                preparedStatement.setString(9, value.getAtt());
//                preparedStatement.setString(10, value.getGain());
//                preparedStatement.setString(11, value.getPointNum());
//                preparedStatement.setString(12, value.getSegmentNum());
//                preparedStatement.setString(13, value.getBtraceName());
//                preparedStatement.setString(14, value.getEthreshold());
//                preparedStatement.setString(15, value.getIndexName());
//                preparedStatement.setString(16, value.getCompoundCode());
//                preparedStatement.setString(17, value.getData());
//                preparedStatement.executeUpdate();
//
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//
//        }*/
//
//        }
//    }
//
//    private static class ByteDeSerializer implements org.apache.flink.api.common.serialization.DeserializationSchema<byte[]>, org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema<byte[]> {
//        @Override
//        public void open(InitializationContext context) throws Exception {
//            DeserializationSchema.super.open(context);
//        }
//
//        @Override
//        public byte[] deserialize(byte[] bytes) throws IOException {
//            return new byte[0];
//        }
//
//        @Override
//        public boolean isEndOfStream(byte[] bytes) {
//            return false;
//        }
//
//        @Override
//        public byte[] deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
//            return new byte[0];
//        }
//
//        @Override
//        public TypeInformation<byte[]> getProducedType() {
//            return null;
//        }
//    }
//    @Data
//    @NoArgsConstructor
//    @AllArgsConstructor
//    private  class Pcap {
//        private String devIp;
//        private int devPort;
//        private String meteDataId;
//        private int sequenceNumber;
//        private int secondFrag;
//        private int firstFrag;
//        private int meteType;
//        private int flowMatch;
//        private int flowId;
//        private String metePort;
//        private String srcIp;            //  源IP
//        private String srcPort;          //  源端口
//        private String destIp;            //  目标Ip
//        private String destPort;          //  目标端口
//        private String protocol;         //  协议
//        private String srcMac;           //  源mac
//        private String destMac;           //  目的mac
//        private String ethType;
//        private long timeStamp;           //时间戳
//        private byte[] packetData;
//        private String sNNum;           //设备序列号
//    }
//}
//}
