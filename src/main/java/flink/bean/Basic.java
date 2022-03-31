package flink.bean;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Basic {
    private String destMac;
    private String srcIp;
    private String destIp;
    private String srcPort;
    private String destPort;
    private String protocolType;
    private String byteNumber;
    private String packageNumber;
    private String verifyMessageBodyType;
    private String verifyTypeId;
    private String loadLength;
    private String verifyFunctionModuleCode;
}
