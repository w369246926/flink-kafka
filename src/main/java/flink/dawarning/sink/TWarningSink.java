package flink.dawarning.sink;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import ru.yandex.clickhouse.BalancedClickhouseDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class TWarningSink extends RichSinkFunction<JSONObject> {
    String sql= "";
    private Statement stmt;
    private Connection conn;
    private PreparedStatement preparedStatement;
    String jdbcUrl = "jdbc:clickhouse://10.10.42.241:8123/default";//39.96.136.60:8123,,10.10.41.242:8123,10.10.41.251:8123
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        //加载JDBC驱动
        Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
        //获取数据库连接
        conn = new BalancedClickhouseDataSource(jdbcUrl).getConnection("default", "123456");
        stmt = conn.createStatement();
        //preparedStatement = conn.prepareStatement(sql);
    }
    @Override
    public void close() throws Exception {
        super.close();
        if (conn != null) {
            conn.close();
        }
    }
    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        Object size = value.get("size");
        String tablename = "";
        if (size == null){
             tablename = "bus_twarning";
        }else{ tablename = "bus_warning";}
        String sql= "";
        StringBuilder columns = new StringBuilder("uuid,date,currenttime,messageProtocolVersion,messageDeviceTypeId,messageProductId,messageDeviceDescribe,messageEmbeddedSoftwareVersion," +
                "messageChipVersion,messageDeviceSerialId,messagePackageId,messageLoadLength,messageNumber,messageSplitSerialId," +
                "verifyCode,reserved,pid,");
        StringBuilder values = new StringBuilder();
        values.append("'");
        values.append (value.getOrDefault("uuid","-1")).append("','");
        values.append (value.getOrDefault("date","-1")).append("','");
        values.append (value.getOrDefault("currenttime","-1")).append("','");
        values.append (value.getOrDefault("messageProtocolVersion","-1")).append("','");
        values.append (value.getOrDefault("messageDeviceTypeId","-1")).append("','");
        values.append (value.getOrDefault("messageProductId","-1")).append("','");
        values.append (value.getOrDefault("messageDeviceDescribe","-1")).append("','");
        values.append (value.getOrDefault("messageEmbeddedSoftwareVeeVersion","-1")).append("','");
        values.append (value.getOrDefault("messageChipVersion","-1")).append("','");
        values.append (value.getOrDefault("messageDeviceSerialId","-1")).append("','");
        values.append (value.getOrDefault("messagePackageId","-1")).append("','");
        values.append (value.getOrDefault("messageLoadLength","-1")).append("','");
        values.append (value.getOrDefault("messageNumber","-1")).append("','");
        values.append (value.getOrDefault("messageSplitSerialId","-1")).append("','");
        values.append (value.getOrDefault("verifyCode","-1")).append("','");
        values.append (value.getOrDefault("reserved","-1")).append("','");
        values.append (value.getOrDefault("pid","-1")).append("','");
        //values.append (value.getOrDefault("size","-1")).append("','");
        try {
            //获取所有告警的JSON
            JSONArray basicMessageBasicList = value.getJSONArray("basicMessageBasicList");
            Map map = new HashMap();
            //遍历告警JSON
            for (int i = 0; i < basicMessageBasicList.size(); i++) {
                map = basicMessageBasicList.getJSONObject(i);
                //遍历第 i 个json
                Set<Map.Entry<String, String>> set = map.entrySet();
                for (Map.Entry<String, String> stringStringEntry : set) {
                    columns.append(stringStringEntry.getKey().toString()).append(",");
                    String s = String.valueOf(stringStringEntry.getValue());
                    values.append(s).append("','");
                }
                columns.append("tempVerifyCode,messageSplit");
                values.append (value.getOrDefault("tempVerifyCode","-1")).append("','");
                values.append (value.getOrDefault("messageSplit","-1")).append("'");
                String sqlkey = columns.toString();
                String sqlvalue = values.toString();
                sql = "INSERT INTO default."+tablename+" ( "+ sqlkey+" ) VALUES ( "+sqlvalue +" )";
                //System.out.println("变星表");
                stmt.executeQuery(sql);
            }
        }catch (Exception e){
            System.out.println(e);
        }
    }

}
