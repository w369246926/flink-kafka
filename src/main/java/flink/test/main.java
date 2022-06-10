package flink.test;

public class main {
    public static void main(String[] args) {
        // 1. 键盘录入一个字符串，用 Scanner 实现
        //String s = "{\"device_id\":\"222222222222\",\"device_ip\":\"10.45.69.3\",\"interface_icon\":\"核心交换\",\"data_type\":1,\"time\":1581927006,\"sip\":\"1.2.170.2\",\"sipv6\":\"2015::168\",\"smac\":\"00:50:56:9C:18:D4\",\"sport\":51140,\"dip\":\"1.2.170.5\",\"dipv6\":\"2015::168\",\"dmac\":\"00:16:31:F9:C0:17\",\"dport\":443,\"network_protocol\":1,\"transport_protocol\":4,\"session_protocol\":1,\"app_protocol\":80,\"sess_id\":\"1111111\",\"log_type\":5,\"method\":\"GET\",\"uri\":\"/upload.aspx?Path=/\",\"host\":\"3.3adisk.com\",\"origin\":\"http://3.3adisk.com\",\"cookie\":\"ASP.NET_SessionId=k0tnw045ypzwklniddsorrza;\",\"agent\":\"Mozilla/5.0 (Windows NT 6.1; WW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36\",\"referer\":\"http://3.3adisk.com/upload.aspx?Path=%2f' web_xff='\",\"http_req_header\":\"GET /Host: 1.1.1.1\",\"http_res_code\":200,\"content_type\":\"application/octet-stream\",\"content_length \":1375,\"http_res_heade\":\"200 OK\",\"file_list\":[{123.exe}]}";
        //String s = "file_list:[{123.exe}]{\"device_id\":\"222222222222\",\"device_ip\":\"10.45.69.3\",\"interface_icon\":\"核心交换\",\"data_type\":1,\"time\":1581927006,\"sip\":\"1.2.170.2\",\"sipv6\":\"2015::168\",\"smac\":\"00:50:56:9C:18:D4\",\"sport\":51140,\"dip\":\"1.2.170.5\",\"dipv6\":\"2015::168\",\"dmac\":\"00:16:31:F9:C0:17\",\"dport\":443,\"network_protocol\":1,\"transport_protocol\":4,\"session_protocol\":1,\"app_protocol\":80,\"sess_id\":\"1111111\",\"log_type\":5,\"method\":\"GET\",\"uri\":\"/upload.aspx?Path=/\",\"host\":\"3.3adisk.com\",\"origin\":\"http://3.3adisk.com\",\"cookie\":\"ASP.NET_SessionId=k0tnw045ypzwklniddsorrza;\",\"agent\":\"Mozilla/5.0 (Windows NT 6.1; WW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36\",\"referer\":\"http://3.3adisk.com/upload.aspx?Path=%2f' web_xff='\",\"http_req_header\":\"GET /Host: 1.1.1.1\",\"http_res_code\":200,\"content_type\":\"application/octet-stream\",\"content_length \":1375,\"http_res_heade\":\"200 OK\",\"file_list\":[{123.exe}]}";
        //System.out.println("请输入:");
        // 2. 替换敏感词
        //String result = s.replace("[","\"").replace("]","\"").replace("'","|");
        // 3. 输出结果
        //System.out.println(result);
        String s = "1,2,3,4";
        String[] split = s.split(",");
        for (int i = 0; i < split.length; i++) {
            System.out.println(i);
            System.out.println(split[i]);
            System.out.println(split.length);
            System.out.println("123123");
        }
    }
}
