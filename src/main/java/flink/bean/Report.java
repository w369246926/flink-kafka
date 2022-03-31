package flink.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Report {
        private String uuid;
        private Long date;
        private String messageProtocolVersion;
        private String messageDeviceTypeId;
        private String messageProductId;
        private String messageDeviceDescribe;
        private String messageEmbeddedSoftwareVersion;
        private String messageChipVersion;
        private String messageDeviceSerialId;
        private String messagePackageId;
        private String messageLoadLength;
        private String messageNumber;
        private String messageSplitSerialId;
        private String verifyCode;
        private String reserved;
        private List basicMessageBasicList;
        private String tempVerifyCode;
        private String messageSplit;
    }
