package flink.FatMap;

import flink.pojo.Pcap;
import net.sourceforge.jpcap.net.ICMPPacket;
import net.sourceforge.jpcap.net.TCPPacket;
import net.sourceforge.jpcap.net.UDPPacket;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * RichFlatMapFunction -> FlatMapFunction 的富函数 有生命周期的
 * byte[] -> 输入类型
 * Tuple2<String,Pcap> -> 输出类型
 */
public class PcapFlatMap extends RichFlatMapFunction<byte[],Pcap> {
    private MapState<String, Pcap> mapState;
    private  static Logger logger = LoggerFactory.getLogger(PcapFlatMap.class);

    @Override
    public void open(Configuration parameters) throws Exception {
        MapStateDescriptor<String, Pcap> dec = new MapStateDescriptor<>("map-state", String.class, Pcap.class);
        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.seconds(60))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired).build();
        dec.enableTimeToLive(ttlConfig);
        mapState = getRuntimeContext().getMapState(dec);
    }

    @Override
    public void flatMap(byte[] value, Collector<Pcap> out) throws Exception {
        ByteArrayInputStream in = new ByteArrayInputStream(value);
        byte[] buffer_4= new byte[4];
//        byte[] buffer_2= new byte[2];
        //解析发数据的Ip
//        in.read(buffer_4);
//        String devIp = "";
        //解析发数据的 port
//        in.read(buffer_2);
//        int devPort = 0;
        //解析metaData
        in.read(buffer_4);
        //0-4字节
        String meteDataId = getMeteDataId(buffer_4);
        int sequenceNumber = Integer.parseInt(meteDataId.substring(0,10),2);
        int secondFrag = Integer.parseInt(meteDataId.substring(10,11));
        int firstFrag = Integer.parseInt(meteDataId.substring(11,12));
        int meteType =  Integer.parseInt(meteDataId.substring(12,14),2);
        int flowMatch =Integer.parseInt( meteDataId.substring(14,15));
        int flowId =Integer.parseInt( meteDataId.substring(15,29),2);
        int sourcePort = Integer.parseInt(meteDataId.substring(29,32),2);
        in.read(buffer_4);
        //4-8字节
//        解析
//        Package_index  24
//        SN_device 2
//        SN_produc 4
//        SN_batch 2
        String meteDataId1 = getMeteDataId(buffer_4);
        int Package_index  = Integer.parseInt(meteDataId1.substring(0,24),2);
        String SN_device  = meteDataId1.substring(24,26);
        String SN_produc  = meteDataId1  .substring(26,30);
        String SN_batch  = meteDataId1.substring(30,32);
        if(SN_produc.equals("0000")){
            SN_produc="7100";
        }else if (SN_produc.equals("0001")){
            SN_produc="7020";
        }
        in.read(buffer_4);
        //8-12字节
        String SNNumTmp = new String(buffer_4,"utf-8");
//        logger.info("SNNumTmp"+SNNumTmp);
        // 生成最终的设备序列号  +"-"+
        String SNNum ="JAJX"+SN_device+SN_produc+SN_batch+SNNumTmp;
        logger.info("SNNum"+SNNum);
        String idCus = SNNum + sequenceNumber;  //自定义组合ID
        Pcap pcap1 = new Pcap(SNNum, meteDataId, sequenceNumber, secondFrag, firstFrag, meteType, flowMatch, flowId, sourcePort);
        if(firstFrag == 0 && secondFrag == 0 ){// 无分片  直接存储
            Map<String, Object> pcapInfo = getPcapInfo(in);
            if(pcapInfo==null ){
//                logger.info("pcapInfo==null  数据格式错误 "+(pcapInfo==null));
                return ;
            }
            BeanUtils.populate(pcap1,pcapInfo);
//            logger.info("pcapInfo 0"+pcapInfo.toString());
            out.collect(pcap1);
        }else if(firstFrag < secondFrag ){// 第二分片 直接存储
            //判断有没有第一个分片
            Pcap pcap = mapState.get(idCus+ "01");
            byte[] buffer_all = new byte[in.available()];
            if(pcap == null ){//没有分片  进行存储
                pcap1.setPacketData(buffer_all);
                mapState.put(idCus +  "10",pcap1);
                logger.info("pcapInfo 1");
                return;
            }
            mapState.remove(idCus +  "01");
            pcap1.setFirstFrag(1);// SecondFrag 和 FirstFrag均为1 表示合并包完毕
            byte[] data = pcap.getPacketData();
            in.read(buffer_all);  //  读取第2分片中数据
            byte[] data1 = ArrayUtils.addAll(data, buffer_all);
            Map<String, Object> pcapInfo = getPcapInfo(new ByteArrayInputStream(data1));
            if(pcapInfo==null ){
//                logger.info("pcapInfo==null" + (pcapInfo==null));
                return ;
            }
            BeanUtils.populate(pcap1,pcapInfo);
            logger.info("pcapInfo 2" + pcapInfo.toString());
            out.collect(pcap1);
        }else {//第一分片
            //判断有没有第二个分片
            Pcap pcap = mapState.get(idCus+ "10");
            byte[] buffer_all = new byte[in.available()];
            if(pcap == null ){//  没有分片 进行存储
                pcap1.setPacketData(buffer_all);
                mapState.put(idCus+ "01",pcap1);
                logger.info("pcapInfo 1");
                return ;
            }
            mapState.remove(idCus+"10");
            //分片重组
            byte[] data = pcap.getPacketData();
            pcap1.setSecondFrag(1);// SecondFrag 和 FirstFrag均为1 表示合并包完毕
            in.read(buffer_all);  //  读取第2分片中数据
            byte[] data1 = ArrayUtils.addAll(buffer_all, data);
            Map<String, Object> pcapInfo = getPcapInfo(new ByteArrayInputStream(data1));
            if(pcapInfo==null ){
//                logger.info("pcapInfo==null"+(pcapInfo==null));
                return ;
            }
            BeanUtils.populate(pcap1,pcapInfo);
            logger.info("pcapInfo 1"+pcapInfo.toString());
            out.collect(pcap1);
        }
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
//            if (in.available() < 28) {
//                logger.info("in.available()" + in.available());
//              return null;
//            }
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
            } else { //其他
//                if(buffer_all.length>9){
//                    logger.info("buffer_all[9]"+buffer_all[9]);
//                }
                return null;
            }
            return map;
        } catch (Exception e) {
            e.printStackTrace();
//            logger.info("ExceptionPcapFlatMap" + e.getMessage());
            return  null;
        }
//        BeanUtils.populate
    }

    /** 将给定的字节数组转换成IPV4的十进制分段表示格式的ip地址字符串 */
    public  String bin2Ipv4(byte[] addr) {
        String ip = "";
        for (int i = 0; i < addr.length; i++) {
            ip += (addr[i] & 0xFF) + ".";
        }
        return ip.substring(0, ip.length() - 1);
    }
    public  String byteToHexEthType(byte[] bytes){
        String strHex = "";
        StringBuilder sb = new StringBuilder("0x");
        for (int n = 0; n < 2; n++) {
            strHex = Integer.toHexString(bytes[n] & 0xFF);
            sb.append((strHex.length() == 1) ? "0" + strHex : strHex); // 每个字节由两个字符表示，位数不够，高位补0
        }
        return sb.toString().trim();
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

    public static String byteToHexMac(byte[] bytes){
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

    public static String bytesToBin(byte[] b) {
        StringBuilder sBuilder = new StringBuilder();
        for(int i=0;i<b.length;i++) {
            String zero = "00000000";
            String binStr = Integer.toBinaryString(b[i]&0xFF);
            if(binStr.length() < 8) {
                binStr = zero.substring(0, 8 -binStr.length()) + binStr;
            }
            sBuilder.append(binStr);
        }
        return sBuilder.toString();
    }
}
//
//new FlatMapFunction<byte[], Tuple2<String, byte[]>>() {
//@Override
//public void flatMap(byte[] value, Collector<Tuple2<String, byte[]>> out) throws Exception {
//        //从字节数组中  取前两个字节的前10位作为  唯一ID
//        String key = bytesToBin(value, 10);
//        out.collect(new Tuple2(key, value));
//        }
