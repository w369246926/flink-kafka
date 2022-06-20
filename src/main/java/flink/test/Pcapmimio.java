package flink.test;



import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import net.sourceforge.jpcap.net.ICMPPacket;
import net.sourceforge.jpcap.net.TCPPacket;
import net.sourceforge.jpcap.net.UDPPacket;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class Pcapmimio {
    //private  static Logger logger = LoggerFactory.getLogger(Pcapmimio2.class);
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /*env.enableCheckpointing(1000L);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(3);*/
        Map properties = new HashMap();
//        properties.put("zookeeper.connect", "192.168.110.245:2181,192.168.110.40:2181,192.168.110.214:2181");
//        properties.put("bootstrap.servers", "192.168.110.245:6667,192.168.110.40:6667,192.168.110.214:6667");
        //properties.put("zookeeper.connect", "10.10.41.251:2181,10.10.41.242:2181,10.10.41.243:2181");
        properties.put("bootstrap.servers", "10.10.41.251:9092,10.10.41.242:9092,10.10.41.243:9092");
        properties.put("group.id", "consumerpcap22 ");
//        properties.put("enable.auto.commit", "true");
//        properties.put("auto.commit.interval.ms", "1000");
//        properties.put("max.poll.records", "1000");
//        properties.put("auto.offset.reset", "latest");//latest  earliest
//        properties.put("session.timeout.ms", "30000");
//        properties.put("heartbeat.interval.ms", "10000");
//        properties.put("max.partition.fetch.bytes", "104857600");
//        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        properties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
//        properties.put("fetch.max.wait.ms", "500");
//        properties.put("fetch.min.bytes", "3655360");
//        properties.put("topic", "packdata");

        ParameterTool parameterTool = ParameterTool.fromMap(properties);
        FlinkKafkaConsumer010<byte[]> transction = new FlinkKafkaConsumer010("packdata",new ByteDeSerializer(), parameterTool.getProperties());
//        transction.setStartFromEarliest();
//        transction.setStartFromGroupOffsets();
        DataStream<byte[]> transction1 = env.addSource(transction).name("Kafka数据源");
        DataStream<Pcap2> dataStream = transction1.keyBy(new KeySelector<byte[], String>() {
            @Override
            public String getKey(byte[] value) throws Exception {
                byte[] data = new byte[4];
                System.arraycopy(value, 8, data, 0, 4);//设备序列号（32bit）
                ByteArrayInputStream in = new ByteArrayInputStream(data);
                byte[] buffer_2= new byte[2];
                System.arraycopy(value, 24, buffer_2, 0, 2);//原始包序列号
                in.read(buffer_2);
                String sequenceNumber =  getMeteDataId(buffer_2);
                String idCus = sequenceNumber;  //自定义组合ID =设备序列号+包序号
                return idCus;
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
                .flatMap(new RichFlatMapFunction<byte[], Pcap2>() {
                    @Override
                    public void flatMap(byte[] value, Collector<Pcap2> out) throws Exception {
                        ByteArrayInputStream in = new ByteArrayInputStream(value);
                        System.out.println(in);
                        byte[] buffer_28= new byte[28];
                        //解析metaData
                        in.read(buffer_28);
                        //0-4字节
                        String meteData = getMeteDataId(buffer_28);

                        System.out.println(meteData);

                        String Second_frag = meteData.substring(0,1);//第二个分片 1
                        String First_frag = meteData.substring(1,2);//第一个分片  2
                        int Module_code = Integer.parseInt(meteData.substring(2,6),10);//详见附件功能模块定义  3
                        int Source_code =  Integer.parseInt(meteData.substring(6,10),2);//详见附件发送源模块定义  4
                        String reserveone = meteData.substring(10,30);//保留  5
                        int Type =Integer.parseInt(meteData.substring(30,32),2);//类型  6
                        int Pkt_number  = Integer.parseInt(meteData.substring(32,56),2);//会话中的报文序号  7
                        int SN_device  = Integer.parseInt(meteData.substring(56,58),2);//设备代号  8
                        int SN_produc  = Integer.parseInt(meteData.substring(58,62),2);//产品型号  9
                        int SN_batch  = Integer.parseInt(meteData.substring(62,64),2);//批次  10
                        String SN_number = meteData.substring(64,96);  //设备序列号（32bit）  11
                        String Policy_match  = meteData.substring(96,97);//1表示匹配策略，0表示未匹配策略 12
                        int Policy_id  = Integer.parseInt(meteData.substring(97,111),2);//匹配的策略号  13
                        int Source_port  = Integer.parseInt(meteData .substring(111,118),2);//接 收包源端口号  14
                        String Time_stamp = meteData.substring(118,160);//时间  15
                        int Flow_index = Integer.parseInt(meteData.substring(160,192),2);  //16
                        int Sequence_number  = Integer.parseInt(meteData.substring(192,208),2);//原始包序列号  17
                        String reservetow  = meteData.substring(208,224);//匹配的策略号  18
                        Pcap2 pcap2 = new Pcap2(Second_frag, First_frag, Module_code, Source_code, reserveone, Type, Pkt_number, SN_device, SN_produc,SN_batch,SN_number,Policy_match,Policy_id,Source_port,Time_stamp,Flow_index,Sequence_number,reservetow);
                        byte[] buffer_all = new byte[in.available()];
                        pcap2.setPacketData(buffer_all);
                        Map<String, Object> pcapInfo = getPcapInfo(in);
                        if(pcapInfo==null ){
//                logger.info("pcapInfo==null  数据格式错误 "+(pcapInfo==null));
                            return ;
                        }
                        BeanUtils.populate(pcap2,pcapInfo);
                        out.collect(pcap2);
                    }

                    private Map<String,Object> getPcapInfo( ByteArrayInputStream in ) {
                        Map<String,Object>  map = new HashMap<String,Object>();
                        try {
                            byte[] buffer_6 = new byte[6];
                            byte[] buffer_2 = new byte[2];
                            in.read(buffer_6);
                            String dstMac = byteToHexMac(buffer_6);
                            in.read(buffer_6);
                            String srcMac = byteToHexMac(buffer_6);
                            in.read(buffer_2);
                            String ethType = byteToHexEthType(buffer_2);
                            map.put("destMac", dstMac);
                            map.put("srcMac", srcMac);
                            map.put("ethType", ethType);//ethType IPv4: 0x0800
                            byte[] buffer_all = new byte[in.available()];
                            in.read(buffer_all);
                            if (buffer_all[9] == 17) {// udp
                                UDPPacket udpPacket = new UDPPacket(0, buffer_all);
                                int protocol = udpPacket.getProtocol();
                                int version = udpPacket.getVersion();
                                String dstIp = udpPacket.getDestinationAddress();
                                int dstPort = udpPacket.getDestinationPort();
                                String srcIP = udpPacket.getSourceAddress();
                                int srcPort = udpPacket.getSourcePort();
                                byte[] packetData = udpPacket.getData();
                                map.put("protocol", "UDP");
                                map.put("destIp", dstIp);
                                map.put("destPort", dstPort);
                                map.put("srcIp", srcIP);
                                map.put("srcPort", srcPort);
                                map.put("packetData", packetData);
                            } else if (buffer_all[9] == 6) {//  tcp
                                TCPPacket tcpPacket = new TCPPacket(0, buffer_all);
                                int protocol = tcpPacket.getProtocol();
                                int version = tcpPacket.getVersion();
                                String dstIp = tcpPacket.getDestinationAddress();
                                int dstPort = tcpPacket.getDestinationPort();
                                String srcIP = tcpPacket.getSourceAddress();
                                int srcPort = tcpPacket.getSourcePort();
                                byte[] packetData = tcpPacket.getData();
                                int urgentPointer = tcpPacket.getUrgentPointer();
                                long acknowledgmentNumber = tcpPacket.getAcknowledgmentNumber();
                                map.put("protocol", "TCP");
                                map.put("destIp", dstIp);
                                map.put("destPort", dstPort);
                                map.put("srcIp", srcIP);
                                map.put("srcPort", srcPort);
                                map.put("packetData", packetData);
                            } else if (buffer_all[9] == 1) { // ICMP
                                ICMPPacket icmpPacket = new ICMPPacket(0, buffer_all);
                                int protocol = icmpPacket.getProtocol();
                                int version = icmpPacket.getVersion();
                                String dstIp = icmpPacket.getDestinationAddress();
                                String srcIP = icmpPacket.getSourceAddress();
                                byte[] packetData = icmpPacket.getData();
                                map.put("protocol", "ICMP");
                                map.put("destIp", dstIp);
                                map.put("srcIp", srcIP);
                                map.put("packetData", packetData);
                            } else {
                                return null;
                            }
                            return map;
                        } catch (Exception e) {
                            e.printStackTrace();
                            return  null;
                        }
                    }
                    /** 将给定的字节数组转换成IPV4的十进制分段表示格式的ip地址字符串 */
                    public  String byteToHexEthType(byte[] bytes){
                        String strHex = "";
                        StringBuilder sb = new StringBuilder("0x");
                        for (int n = 0; n < 2; n++) {
                            strHex = Integer.toHexString(bytes[n] & 0xFF);
                            sb.append((strHex.length() == 1) ? "0" + strHex : strHex); // 每个字节由两个字符表示，位数不够，高位补0
                        }
                        return sb.toString().trim();
                    }
                    public  String byteToHexMac(byte[] bytes){
                        String strHex = "";
                        if(bytes.length!=6) {
                            return null;
                        }
                        StringBuilder sb = new StringBuilder("");
                        for (int n = 0; n < 6; n++) {
                            strHex = Integer.toHexString(bytes[n] & 0xFF);
                            sb.append((strHex.length() == 1) ? "0" + strHex : strHex); // 每个字节由两个字符表示，位数不够，高位补0
                            if(n != 5) {
                                sb.append(":");
                            }
                        }
                        return sb.toString().trim();
                    }
                    public String getMeteDataId(byte[] b) {
                        StringBuilder sBuilder = new StringBuilder();
                        for (int i = 0; i < b.length; i++) {
                            String zero = "00000000";
                            String binStr = Integer.toBinaryString(b[i] & 0xFF);
                            if (binStr.length() < 8) {
                                binStr = zero.substring(0, 8 - binStr.length()) + binStr;
                            }
                            sBuilder.append(binStr);
                        }
                        return sBuilder.toString();
                    }
                });
                //.setParallelism(1);
        System.out.println("sink");

        dataStream.addSink(new miniosink());
        env.execute();
    }

    private static class miniosink extends RichSinkFunction<Pcap2> {
        MinioClient minioClient;
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            minioClient = MinioClient.builder()
                            .endpoint("http://10.10.41.251:9090")
                            .credentials("adminminio", "admin123456")
                            .build();
        }

        @Override
        public void invoke(Pcap2 value, Context context) throws Exception {
            //String time_stamp = value.getTime_stamp();
            long l = System.currentTimeMillis();
            String time_stamp = String.valueOf(l);
            String str = value.toString();
            System.out.println(str);
            InputStream inputStream = new ByteArrayInputStream(str.getBytes());

            minioClient.putObject(PutObjectArgs.builder().bucket("data").object(time_stamp)
                    .stream(inputStream, -1, 10485760).build());
        }

        @Override
        public void close() throws Exception {
            super.close();

        }
    }



    public static class Pcap2 implements Serializable {
        private String Second_frag;//1表示原始包第二个分片，0表示非第二个分片
        private String First_frag;//1表示原始包第一个分片，0表示非第一个分片
        private int Module_code;//详见附件功能模块定义
        private int Source_code;//详见附件发送源模块定义
        private String reserveone;//
        private int Type;//port_cfg. arp_trap寄存器配置为1时，ARP包send to CPU；1表示远程镜像包；2表示用户主机重定向到监控端的包；3表示设备CPU发送监控端的包
        private int Pkt_number;//会话中的报文序号
        private int SN_device;//设备代号（2bit）：①盒子产品，代号01；②标准机架产品，代号10；③板件产品，代号11
        private int SN_produc;//产品型号（4bit）①7100，型号0000；②7020，代号0001
        private int SN_batch;//批次（2bit） ①第一批次 01；②第二批次 10③第一批次 11
        private String SN_number;//设备序列号（32bit）
        private String Policy_match;//1表示匹配策略，0表示未匹配策略
        private int Policy_id;//匹配的策略号
        private int Source_port;//接 收包源端口号
        private String Time_stamp;//时间戳（该原始流量包的唯一标识，做数据关联）
        private int Flow_index;//会话索引号
        private int Sequence_number;//原始包序列号，来自Sequence_number寄存器，每一个原始包对应一个sequence_number，如果没有分片，则first_frag和second_frag全为0或者1，如果原始包有分片，则两个分片的sequence_number相同，并且第一个分片的first_frag为1，第二个分片的second_frag为1，监控端软件需要剥离外层UDP后重组原始包，如果包存在乱序，则第二个分片先到监控端后，还需要等待第一个分片到达后才能重组
        private String reservetow;
        private byte[] PacketData;//被封装的明文原始包

        public Pcap2() {
        }

        public Pcap2(String second_frag, String first_frag, int module_code, int source_code, String reserveone, int type, int pkt_number, int SN_device, int SN_produc, int SN_batch, String SN_number, String policy_match, int policy_id, int source_port, String time_stamp, int flow_index, int sequence_number, String reservetow, byte[] packetData) {
            Second_frag = second_frag;
            First_frag = first_frag;
            Module_code = module_code;
            Source_code = source_code;
            this.reserveone = reserveone;
            Type = type;
            Pkt_number = pkt_number;
            this.SN_device = SN_device;
            this.SN_produc = SN_produc;
            this.SN_batch = SN_batch;
            this.SN_number = SN_number;
            Policy_match = policy_match;
            Policy_id = policy_id;
            Source_port = source_port;
            Time_stamp = time_stamp;
            Flow_index = flow_index;
            Sequence_number = sequence_number;
            this.reservetow = reservetow;
            PacketData = packetData;
        }

        public Pcap2(String second_frag, String first_frag, int module_code, int source_code, String reserveone, int type, int pkt_number, int sn_device, int sn_produc, int sn_batch, String sn_number, String policy_match, int policy_id, int source_port, String time_stamp, int flow_index, int sequence_number, String reservetow) {

        }

        public String getSecond_frag() {
            return Second_frag;
        }

        public void setSecond_frag(String second_frag) {
            Second_frag = second_frag;
        }

        public String getFirst_frag() {
            return First_frag;
        }

        public void setFirst_frag(String first_frag) {
            First_frag = first_frag;
        }

        public int getModule_code() {
            return Module_code;
        }

        public void setModule_code(int module_code) {
            Module_code = module_code;
        }

        public int getSource_code() {
            return Source_code;
        }

        public void setSource_code(int source_code) {
            Source_code = source_code;
        }

        public String getReserveone() {
            return reserveone;
        }

        public void setReserveone(String reserveone) {
            this.reserveone = reserveone;
        }

        public int getType() {
            return Type;
        }

        public void setType(int type) {
            Type = type;
        }

        public int getPkt_number() {
            return Pkt_number;
        }

        public void setPkt_number(int pkt_number) {
            Pkt_number = pkt_number;
        }

        public int getSN_device() {
            return SN_device;
        }

        public void setSN_device(int SN_device) {
            this.SN_device = SN_device;
        }

        public int getSN_produc() {
            return SN_produc;
        }

        public void setSN_produc(int SN_produc) {
            this.SN_produc = SN_produc;
        }

        public int getSN_batch() {
            return SN_batch;
        }

        public void setSN_batch(int SN_batch) {
            this.SN_batch = SN_batch;
        }

        public String getSN_number() {
            return SN_number;
        }

        public void setSN_number(String SN_number) {
            this.SN_number = SN_number;
        }

        public String getPolicy_match() {
            return Policy_match;
        }

        public void setPolicy_match(String policy_match) {
            Policy_match = policy_match;
        }

        public int getPolicy_id() {
            return Policy_id;
        }

        public void setPolicy_id(int policy_id) {
            Policy_id = policy_id;
        }

        public int getSource_port() {
            return Source_port;
        }

        public void setSource_port(int source_port) {
            Source_port = source_port;
        }

        public String getTime_stamp() {
            return Time_stamp;
        }

        public void setTime_stamp(String time_stamp) {
            Time_stamp = time_stamp;
        }

        public int getFlow_index() {
            return Flow_index;
        }

        public void setFlow_index(int flow_index) {
            Flow_index = flow_index;
        }

        public int getSequence_number() {
            return Sequence_number;
        }

        public void setSequence_number(int sequence_number) {
            Sequence_number = sequence_number;
        }

        public String getReservetow() {
            return reservetow;
        }

        public void setReservetow(String reservetow) {
            this.reservetow = reservetow;
        }

        public byte[] getPacketData() {
            return PacketData;
        }

        public void setPacketData(byte[] packetData) {
            PacketData = packetData;
        }
    }

    public static class ByteDeSerializer implements DeserializationSchema<byte[]> {

        @Override
        public byte[] deserialize(byte[] message) {
            return message;
        }

        @Override
        public boolean isEndOfStream(byte[] nextElement) {
            return false;
        }

        @Override
        public TypeInformation<byte[]> getProducedType() {
            return TypeInformation.of(new TypeHint<byte[]>(){});
        }
    }


}
