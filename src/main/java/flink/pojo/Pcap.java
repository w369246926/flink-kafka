package flink.pojo;

public class Pcap {
    private String devIp;
    private int devPort;
    private String meteDataId;
    private int sequenceNumber;
    private int secondFrag;
    private int firstFrag;
    private int meteType;
    private int flowMatch;
    private int flowId;
    private String metePort;
    private String srcIp;            //  源IP
    private String srcPort;          //  源端口
    private String destIp;            //  目标Ip
    private String destPort;          //  目标端口
    private String protocol;         //  协议
    private String srcMac;           //  源mac
    private String destMac;           //  目的mac
    private String ethType;
    private long timeStamp;           //时间戳
    private byte[] packetData;
    private String sNNum;           //设备序列号

    public String getSNNum() {
        return sNNum;
    }

    public void setSNNum(String SNNum) {
        this.sNNum = SNNum;
    }

    public Pcap(){
        super();
        this.timeStamp = System.currentTimeMillis();
    }
    public Pcap(String devIp, int devPort, String meteDataId, int sequenceNumber, int secondFrag, int firstFrag, int meteType, int flowMatch, int flowId, String metePort, String srcIp, String srcPort, String destIp, String destPort, String protocol, String srcMac, String destMac, byte[] packetData) {
        this.devIp = devIp;
        this.devPort = devPort;
        this.meteDataId = meteDataId;
        this.sequenceNumber = sequenceNumber;
        this.secondFrag = secondFrag;
        this.firstFrag = firstFrag;
        this.meteType = meteType;
        this.flowMatch = flowMatch;
        this.flowId = flowId;
        this.metePort = metePort;
        this.srcIp = srcIp;
        this.srcPort = srcPort;
        this.destIp = destIp;
        this.destPort = destPort;
        this.protocol = protocol;
        this.srcMac = srcMac;
        this.destMac = destMac;
        this.packetData = packetData;
        this.timeStamp = System.currentTimeMillis();
    }

    public Pcap(String devIp, int devPort, String meteDataId, int sequenceNumber, int secondFrag, int firstFrag, int meteType, int flowMatch, int flowId, String metePort) {
        this.devIp = devIp;
        this.devPort = devPort;
        this.meteDataId = meteDataId;
        this.sequenceNumber = sequenceNumber;
        this.secondFrag = secondFrag;
        this.firstFrag = firstFrag;
        this.meteType = meteType;
        this.flowMatch = flowMatch;
        this.flowId = flowId;
        this.metePort = metePort;
        this.timeStamp = System.currentTimeMillis();
    }

//    public Pcap(String devIp, int devPort, String meteDataId, int sequenceNumber, int secondFrag, int firstFrag, int meteType, int flowMatch, int flowId, int sourcePort) {
public Pcap(String SNNum, String meteDataId, int sequenceNumber, int secondFrag, int firstFrag, int meteType, int flowMatch, int flowId, int sourcePort) {
        this.meteDataId = meteDataId;
        this.sequenceNumber = sequenceNumber;
        this.secondFrag = secondFrag;
        this.firstFrag = firstFrag;
        this.meteType = meteType;
        this.flowMatch = flowMatch;
        this.flowId = flowId;
        this.metePort = metePort;
        this.sNNum = SNNum;
        this.timeStamp = System.currentTimeMillis();
    }

    public String getDevIp() {
        return devIp;
    }

    public void setDevIp(String devIp) {
        this.devIp = devIp;
    }

    public int getDevPort() {
        return devPort;
    }

    public void setDevPort(int devPort) {
        this.devPort = devPort;
    }

    public String getMeteDataId() {
        return meteDataId;
    }

    public void setMeteDataId(String meteDataId) {
        this.meteDataId = meteDataId;
    }

    public String getSrcIp() {
        return srcIp;
    }

    public void setSrcIp(String srcIp) {
        this.srcIp = srcIp;
    }

    public int getSecondFrag() {
        return secondFrag;
    }

    public void setSecondFrag(int secondFrag) {
        this.secondFrag = secondFrag;
    }

    public int getFirstFrag() {
        return firstFrag;
    }

    public void setFirstFrag(int firstFrag) {
        this.firstFrag = firstFrag;
    }


    public String getMetePort() {
        return metePort;
    }

    public void setMetePort(String metePort) {
        this.metePort = metePort;
    }


    public String getSrcPort() {
        return srcPort;
    }

    public void setSrcPort(String srcPort) {
        this.srcPort = srcPort;
    }

    public String getDestIp() {
        return destIp;
    }

    public void setDestIp(String destIp) {
        this.destIp = destIp;
    }

    public String getDestPort() {
        return destPort;
    }

    public void setDestPort(String destPort) {
        this.destPort = destPort;
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public String getSrcMac() {
        return srcMac;
    }

    public void setSrcMac(String srcMac) {
        this.srcMac = srcMac;
    }

    public String getDestMac() {
        return destMac;
    }

    public void setDestMac(String destMac) {
        this.destMac = destMac;
    }

    public String getEthType() {
        return ethType;
    }

    public void setEthType(String ethType) {
        this.ethType = ethType;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }

    public int getSequenceNumber() {
        return sequenceNumber;
    }

    public void setSequenceNumber(int sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
    }

    public int getMeteType() {
        return meteType;
    }

    public void setMeteType(int meteType) {
        this.meteType = meteType;
    }

    public int getFlowMatch() {
        return flowMatch;
    }

    public void setFlowMatch(int flowMatch) {
        this.flowMatch = flowMatch;
    }

    public int getFlowId() {
        return flowId;
    }

    public void setFlowId(int flowId) {
        this.flowId = flowId;
    }

    public byte[] getPacketData() {
        return packetData;
    }
    public void setPacketData(byte[] packetData) {
        this.packetData = packetData;
    }

}
