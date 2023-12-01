//package com.dc.cksink;
//
//import cn.hutool.core.util.IdUtil;
//import com.dc.bean.WifiProbeBean;
//import com.dc.utils.SqlUtils;
//import org.apache.flink.api.common.functions.RuntimeContext;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
//import org.apache.flink.types.Row;
//import ru.yandex.clickhouse.BalancedClickhouseDataSource;
//
//import java.sql.Connection;
//import java.sql.PreparedStatement;
//import java.sql.SQLException;
//import java.sql.Statement;
//import java.text.SimpleDateFormat;
//import java.util.*;
//
//
//public  class WifiCKSink extends RichSinkFunction<WifiProbeBean> {
//    private BalancedClickhouseDataSource balancedClickhouseDataSource;
//    private List<WifiProbeBean> wifis = new ArrayList<WifiProbeBean>();
//    private long begin = 0l;
//    private Connection conn = null;
//
//    private PreparedStatement pstmt = null;
//    private int day;
//
//
//    @Override
//    public void open(Configuration parameters) throws Exception {
//        super.open(parameters);
//        String jdbcUrl = "jdbc:clickhouse://10.10.41.225:8123,10.10.41.227:8123,10.10.42.224:8123/default";
//        //加载JDBC驱动
//        Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
//        //获取数据库连接
//        Properties properties = new Properties();
//        properties.setProperty("user","default");
//        properties.setProperty("password","bjbigdata");
//        balancedClickhouseDataSource = new BalancedClickhouseDataSource(jdbcUrl,properties);
//        conn = balancedClickhouseDataSource.getConnection();
//        String sql = clickhouseInsertValue(
//                new String[]{"uuid", "event_type", "time_sta", "time_date", "device_id", "ssid", "mac", "encryption", "terminal_ssid", "terminal_mac", "channel", "primary_classification", "secondary_classification", "power", "distance", "monitoring_site", "index_name", "upside", "down", "duration"},
//                "wifiprobe_local",
//                "default"
//        );
//        pstmt = conn.prepareStatement(sql);
//        day = 3600 * 24 * 1000;
//
//    }
//    @Override
//    public void close() throws Exception {
//        super.close();
//
//        conn.close();
//    }
//    @Override
//    public void invoke(WifiProbeBean value, Context context) {
//        try{
//
//            long end = System.currentTimeMillis();
//            if(end % day==0){
//                throw new RuntimeException("每天0点重新链接数据库");
//            }
//            wifis.add(value);
//            if(wifis.size()>1000 || end-begin>8000){
//
//                for(WifiProbeBean wifi:wifis){
//                    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//                    String timeDate = dateFormat.format(wifi.getTime());
//
//                    pstmt.setObject(1,IdUtil.simpleUUID());
//                    pstmt.setObject(2,wifi.getEventType());
//                    pstmt.setObject(3,wifi.getTime());
//                    pstmt.setObject(4,timeDate);
//                    pstmt.setObject(5,wifi.getDeviceId());
//                    pstmt.setObject(6,wifi.getSsid());
//                    pstmt.setObject(7,wifi.getMac());
//                    pstmt.setObject(8,wifi.getEncryption());
//                    pstmt.setObject(9,wifi.getTerminalSsid());
//                    pstmt.setObject(10,wifi.getTerminalMac());
//                    pstmt.setObject(11,wifi.getChannel());
//                    pstmt.setObject(12,wifi.getPrimaryClassification());
//                    pstmt.setObject(13,wifi.getSecondaryClassification());
//                    pstmt.setObject(14,wifi.getPower());
//                    pstmt.setObject(15,wifi.getDistance());
//                    pstmt.setObject(16,wifi.getMonitoringSite());
//                    pstmt.setObject(17,wifi.getIndexName());
//                    pstmt.setObject(18,wifi.getUpside());
//                    pstmt.setObject(19,wifi.getDown());
//                    pstmt.setObject(20,wifi.getDuration());
//                    pstmt.addBatch();
//                }
//                pstmt.executeBatch();
//
//                conn.commit();
//
//                wifis.clear();
//                begin=end;
//
//
//            }
//        }catch (Exception e){
//            try {
//
//                pstmt.close();
//                conn.close();
//                conn = balancedClickhouseDataSource.getConnection();
//                String sql = clickhouseInsertValue(
//                        new String[]{"uuid", "event_type", "time_sta", "time_date", "device_id", "ssid", "mac", "encryption", "terminal_ssid", "terminal_mac", "channel", "primary_classification", "secondary_classification", "power", "distance", "monitoring_site", "index_name", "upside", "down", "duration"},
//                        "wifiprobe_local",
//                        "default"
//                );
//                conn.prepareStatement(sql);
//            } catch (SQLException e1) {
//                e1.printStackTrace();
//            }
//        }
//    }
//    public  Row generateRow( WifiProbeBean wifi) {
//        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//        String timeDate = dateFormat.format(wifi.getTime());
//        Row row = new Row(20);
//        row.setField(0,IdUtil.simpleUUID());
//        row.setField(1,wifi.getEventType());
//        row.setField(2,wifi.getTime());
//        row.setField(3,timeDate);
//        row.setField(4,wifi.getDeviceId());
//        row.setField(5,wifi.getSsid());
//        row.setField(6,wifi.getMac());
//        row.setField(7,wifi.getEncryption());
//        row.setField(8,wifi.getTerminalSsid());
//        row.setField(9,wifi.getTerminalMac());
//        row.setField(10,wifi.getChannel());
//        row.setField(11,wifi.getPrimaryClassification());
//        row.setField(12,wifi.getSecondaryClassification());
//        row.setField(13,wifi.getPower());
//        row.setField(14,wifi.getDistance());
//        row.setField(15,wifi.getMonitoringSite());
//        row.setField(16,wifi.getIndexName());
//        row.setField(17,wifi.getUpside());
//        row.setField(18,wifi.getDown());
//        row.setField(19,wifi.getDuration());
//        return row;
//    }
//    public static String clickhouseInsertValue(String[] tableColums, String tablename,String dataBaseName){
//        StringBuffer sbCloums = new StringBuffer();
//        StringBuffer sbValues = new StringBuffer();
//        for (String s:tableColums) {
//            sbCloums.append(s).append(",");
//            sbValues.append("?").append(",");
//        }
//        String colums=sbCloums.toString().substring(0,sbCloums.toString().length()-1);
//        String values=sbValues.toString().substring(0,sbValues.toString().length()-1);
//
//        String insertSQL="insert into "+dataBaseName+"."+tablename+" ( "+colums+" ) values ( "+values+")";
//        return insertSQL;
//    }
//import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.security.cert.X509Certificate;

public class HttpsClientExample {
    public static void main(String[] args) throws IOException {
        // 定义 HTTPS 请求 URL
        String requestUrl = "https://example.com/api/resource";

        // 忽略 SSL 证书验证（生产环境中请勿使用此配置，应配置信任的证书）
        try {
            SSLContext sslContext = SSLContext.getInstance("SSL");
            sslContext.init(null, new TrustManager[] { new X509TrustManager() {
                public X509Certificate[] getAcceptedIssuers() {
                    return null;
                }

                public void checkClientTrusted(X509Certificate[] certs, String authType) {
                }

                public void checkServerTrusted(X509Certificate[] certs, String authType) {
                }
            } }, new java.security.SecureRandom());

            HttpsURLConnection.setDefaultSSLSocketFactory(sslContext.getSocketFactory());
        } catch (Exception e) {
            e.printStackTrace();
        }

        // 创建 URL 对象
        URL url = new URL(requestUrl);

        // 打开连接
        HttpsURLConnection connection = (HttpsURLConnection) url.openConnection();

        // 设置请求方法（GET、POST 等）
        connection.setRequestMethod("GET");

        // 获取响应代码
        int responseCode = connection.getResponseCode();
        System.out.println("Response Code: " + responseCode);

        // 读取响应内容
        BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
        String line;
        StringBuilder responseContent = new StringBuilder();

        while ((line = reader.readLine()) != null) {
            responseContent.append(line);
        }
        reader.close();

        // 打印响应内容
        System.out.println("Response Content: " + responseContent.toString());

        // 关闭连接
        connection.disconnect();
    }
}

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;

public class HttpsServerExample {
    public static void main(String[] args) throws IOException {
        // 创建 HTTP 服务器，监听指定端口
        HttpServer server = HttpServer.create(new java.net.InetSocketAddress(8080), 0);

        // 创建上下文路径，并将处理器绑定到路径
        server.createContext("/api/resource", new MyHandler());

        // 启动服务器
        server.start();
    }

    static class MyHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange t) throws IOException {
            // 构建响应内容
            String response = "Hello, this is a secure resource!";

            // 设置响应头
            t.getResponseHeaders().set("Content-Type", "text/plain");
            t.sendResponseHeaders(200, response.length());

            // 获取输出流，并写入响应内容
            OutputStream os = t.getResponseBody();
            os.write(response.getBytes());
            os.close();
        }
    }
}
//}
