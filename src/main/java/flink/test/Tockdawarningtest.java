package flink.test;

import cn.hutool.core.util.IdUtil;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import ru.yandex.clickhouse.BalancedClickhouseDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class Tockdawarningtest {
    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String second = "15";
        if (parameterTool.has("second")) {
            second = parameterTool.get("second");
            System.out.println("指定了归并时间:" + second +"秒");
        } else {
            second = "20";
            System.out.println("设置指定归并时间使用 --second ,没有指定使用默认的:" + second +"秒");
        }
        long wmin = Long.parseLong(second);

        //TODO 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //TODO 1.source
        //准备kafka连接参数
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "192.168.88.161:9092");//集群地址
        props.setProperty("bootstrap.servers", "192.168.88.162:9092");//集群地址
        props.setProperty("bootstrap.servers", "192.168.88.163:9092");//集群地址
        props.setProperty("group.id", "flink");//消费者组id
        props.setProperty("auto.offset.reset", "latest");//latest有offset记录从记录位置开始消费,没有记录从最新的/最后的消息开始消费 /earliest有offset记录从记录位置开始消费,没有记录从最早的/最开始的消息开始消费
        //使用连接参数创建FlinkKafkaConsumer/kafkaSource
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<String>("da_warning", new SimpleStringSchema(), props);
        //使用kafkaSource
        DataStream<String> kafkaDS = env.addSource(kafkaSource).setParallelism(1);
        //TODO 2.transformation
        //1,将kafka,String数据变更成jsonobj并打上时间戳time和UUID
        DataStream<JSONObject> dataStream = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {

            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                if (value.length() < 10 && value.equals("")) {
                    return;
                }
                JSONObject jsonObject = JSONObject.parseObject(value);
                JSONArray basicMessageBasicList = new JSONArray();
                //获取所有告警的JSON
                JSONArray basicMessageBasicListorg = jsonObject.getJSONArray("basicMessageBasicList");
                //System.out.println("指定归并的时间" + wmin);

                //遍历告警JSON
                for (int i = 0; i < basicMessageBasicListorg.size(); i++) {
                    Map map = basicMessageBasicListorg.getJSONObject(i);
                    basicMessageBasicList.add(map);
                    jsonObject.put("basicMessageBasicList", basicMessageBasicList);
                    jsonObject.put("uuid", IdUtil.simpleUUID());
                    long date = System.currentTimeMillis();
                    jsonObject.put("date", date);
                    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//可以方便地修改日期格式
                    String currenttime = dateFormat.format(date);
                    jsonObject.put("currenttime", currenttime);
                    if (jsonObject.get("messageDeviceSerialId").equals("JAJX02710001C061")){
                        System.out.println("123123123");
                    }
//                    Object messageDeviceSerialId = jsonObject.get("messageDeviceSerialId");
//                    String s = messageDeviceSerialId.toString();

                    out.collect(jsonObject);
                    basicMessageBasicList.clear();

                }

            }
        });
        //2,将数据拆分成两个流,1:伪装和标签,2:变形
        OutputTag<JSONObject> camouflages = new OutputTag<JSONObject>("伪装和标签", TypeInformation.of(JSONObject.class));
        OutputTag<JSONObject> camouflageandlabel = new OutputTag<JSONObject>("标签", TypeInformation.of(JSONObject.class));
        OutputTag<JSONObject> transformation = new OutputTag<JSONObject>("变形",TypeInformation.of(JSONObject.class)){};
        SingleOutputStreamOperator<JSONObject> process = dataStream.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject jsonObject, Context context, Collector<JSONObject> collector) throws Exception {
                //获取到数据体basicMessageBasicList
                JSONArray basicMessageBasicList = jsonObject.getJSONArray("basicMessageBasicList");
//                Map map = new HashMap();
                //遍历告警JSON
                for (int i = 0; i < basicMessageBasicList.size(); i++) {
                    Map map1 = basicMessageBasicList.getJSONObject(i);
                    Object verifyFunctionModuleCode = map1.get("verifyFunctionModuleCode");
                    //System.out.println(verifyFunctionModuleCode);
                    if (map1.get("verifyFunctionModuleCode").equals(6)) {
                        map1.put("verifyFunctionModuleCode", "网络变形");
                        if (map1.get("verifyTypeId").equals(1))
                            map1.put("verifyTypeId", "标签目的地址未在白名单告警");
                        else if (map1.get("verifyTypeId").equals(2))
                            map1.put("verifyTypeId", "非法访问跳变地址告警");
                        else if (map1.get("verifyTypeId").equals(5))
                            map1.put("verifyTypeId", "变形会话异常结束告警");
                        else if (map1.get("verifyTypeId").equals(8))
                            map1.put("verifyTypeId", "变形会话");
                        else if (map1.get("verifyTypeId").equals(7))
                            map1.put("verifyTypeId", "变形会话异常结束告警");
                        else if (map1.get("verifyTypeId").equals(0))
                            map1.put("verifyTypeId", "标签校验错误告警");
                        else map1.put("verifyTypeId", "模拟");
                        context.output(transformation, jsonObject);
                    }
                    else if (map1.get("verifyFunctionModuleCode").equals(4)) {
                        map1.put("verifyFunctionModuleCode", "网络隐身");
                        if (map1.get("verifyTypeId").equals(1))
                            map1.put("verifyTypeId", "隐身ARP告警");
                        else if (map1.get("verifyTypeId").equals(0))
                            map1.put("verifyTypeId", "非法访问告警");
                        else map1.put("verifyTypeId", "模拟");
                        context.output(camouflageandlabel, jsonObject);
                    }
                    else if (map1.get("verifyFunctionModuleCode").equals(5)) {
//                        try {
//                            if (jsonObject.get("messageDeviceSerialId").equals("JAJX01710001C100")) {
//                                continue;
//                            }
//                        }catch (Exception e){
//                            System.out.println(e);
//                        }
                        map1.put("verifyFunctionModuleCode", "网络伪装");
                        if (map1.get("verifyTypeId").equals(1)) {
                            map1.put("verifyTypeId", "攻击会话开始告警");
                            map1.put("incidentdescription", "攻击会话建立事件");
                        } else if (map1.get("verifyTypeId").equals(2)) {
                            map1.put("verifyTypeId", "攻击会话正常结束告警");
                            map1.put("incidentdescription", "攻击会话结束事件");
                        } else if (map1.get("verifyTypeId").equals(3)) {
                            map1.put("verifyTypeId", "攻击会话异常结束告警");
                            map1.put("incidentdescription", "IP扫描事件");
                        } else if (map1.get("verifyTypeId").equals(4)) {
                            map1.put("verifyTypeId", "回应主机主动向外发包告警");
                            map1.put("incidentdescription", "基于TCP/UDP的横向探测事件");
                        } else if (map1.get("verifyTypeId").equals(5)) {
                            map1.put("verifyTypeId", "ICMP报文攻击告警");
                            map1.put("incidentdescription", "IP扫描事件");

                        } else if (map1.get("verifyTypeId").equals(7)) {
                            map1.put("verifyTypeId", "回应ICMP包告警");
                            map1.put("incidentdescription", "端口扫描事件");
                        } else if (map1.get("verifyTypeId").equals(8)) {
                            map1.put("verifyTypeId", "回应主机主动向外发送ICMP包告警");
                            map1.put("incidentdescription", "回攻击会话持续事件");
                        } else if (map1.get("verifyTypeId").equals(9)) {
                            map1.put("verifyTypeId", "TCP会话超时告警");
                        } else if (map1.get("verifyTypeId").equals(10)) {
                            map1.put("verifyTypeId", "UDP会话超时告警");
                        } else if (map1.get("verifyTypeId").equals(11)) {
                            map1.put("verifyTypeId", "ICMP会话超时告警");
                        } else if (map1.get("verifyTypeId").equals(12)) {
                            map1.put("verifyTypeId", "TCP会话长连接告警");
                        } else if (map1.get("verifyTypeId").equals(13)) {
                            map1.put("verifyTypeId", "5.5.2 UDP会话长连接告警");
                        } else if (map1.get("verifyTypeId").equals(14)) {
                            map1.put("verifyTypeId", "ARP广播告警");
                        } else if (map1.get("verifyTypeId").equals(15)) {
                            map1.put("verifyTypeId", "回应ARP广播告警");
                        } else if (map1.get("verifyTypeId").equals(0)) {
                            map1.put("verifyTypeId", "扫描未伪装端口告警");
                            map1.put("incidentdescription", "端口扫描事件");
                        }else if (map1.get("verifyTypeId").equals(6)) {
                            map1.put("verifyTypeId", "扫描未伪装端口告警");
                            map1.put("incidentdescription", "基于ICMP的横向探测事件");
                        }else {
                            map1.put("verifyTypeId", "模拟");
                        }
                        context.output(camouflages, jsonObject);
                    }
                    else {
                        map1.put("verifyFunctionModuleCode", "安全标签");
                        if (map1.get("verifyTypeId").equals(1)) {
                            map1.put("verifyTypeId", "安全标签告警");
                        } else if (map1.get("verifyTypeId").equals(2)) {
                            map1.put("verifyTypeId", "标签错误告警");
                        } else {
                            map1.put("verifyTypeId", "模拟");
                        }
                        context.output(camouflageandlabel, jsonObject);
                    }

                }
            }
        });
        //2,将数据拆分成两个流,1:伪装和标签,2:变形
        DataStream<JSONObject> camouflage = process.getSideOutput(camouflages);
        DataStream<JSONObject> label1 = process.getSideOutput(camouflageandlabel);
        DataStream<JSONObject> transformation2 = process.getSideOutput(transformation);

        //3,伪装和标签直接存储到A表,结束
        label1.addSink(new ckSinkA());

        //5,变形进行按照时间归并:5分钟归并一次,取时间最大值,并存储到A表
        SingleOutputStreamOperator<JSONObject> apply = transformation2.keyBy(t -> {
            //Map map = new HashMap();
            Object verifyTypeId = null;
            JSONArray basicMessageBasicList = t.getJSONArray("basicMessageBasicList");
            for (int i = 0; i < basicMessageBasicList.size(); i++) {
                Map map = basicMessageBasicList.getJSONObject(i);
                verifyTypeId = map.get("verifyTypeId");
            }
            return verifyTypeId;
        })
                .window(TumblingProcessingTimeWindows.of(Time.seconds(wmin)))
                .apply(new WindowFunction<JSONObject, JSONObject, Object, TimeWindow>() {
                    @Override
                    public void apply(Object o, TimeWindow timeWindow, Iterable<JSONObject> iterable, Collector<JSONObject> collector) throws Exception {

                        //UUID pid = UUID.randomUUID();//归并之后的ID
                        String pid = "";
                        int count = 0;
                        JSONObject jsonone = null;
                        String date="";
                        for (JSONObject jsonObject : iterable) {
                            if (count == 0) {
                                jsonone = jsonObject;
                                //jsonone.put("pid", pid);
                                Object uuid = jsonone.get("uuid");
                                pid = String .valueOf(uuid);
                                break;
                            }
                        }
                        for (JSONObject jsonObject : iterable) {
                            jsonObject.put("pid", pid);
                            Object date1 = jsonObject.get("date");
                            date = String.valueOf(date1);
                            collector.collect(jsonObject);
                            count++;
                        }
                        jsonone.put("date", date);
                        jsonone.put("size",count);
                        collector.collect(jsonone);
                    }
                });
        //6,伪装进行按照时间归并:5分钟归并一次,取时间最大值,并存储到A表
        SingleOutputStreamOperator<JSONObject> camouflageapply = camouflage.keyBy(t -> {
                    //Map map = new HashMap();
                    Object verifyTypeId = null;
                    JSONArray basicMessageBasicList = t.getJSONArray("basicMessageBasicList");
                    for (int i = 0; i < basicMessageBasicList.size(); i++) {
                        Map map = basicMessageBasicList.getJSONObject(i);
                        verifyTypeId = map.get("verifyTypeId");
                    }
                    return verifyTypeId;
                })
                .window(TumblingProcessingTimeWindows.of(Time.seconds(wmin)))
                .apply(new WindowFunction<JSONObject, JSONObject, Object, TimeWindow>() {
                    @Override
                    public void apply(Object o, TimeWindow timeWindow, Iterable<JSONObject> iterable, Collector<JSONObject> collector) throws Exception {

                        //UUID pid = UUID.randomUUID();//归并之后的ID
                        String pid = "";
                        int count = 0;
                        JSONObject jsonone = null;
                        String date="";
                        for (JSONObject jsonObject : iterable) {
                            if (count == 0) {
                                jsonone = jsonObject;
                                //jsonone.put("pid", pid);
                                Object uuid = jsonone.get("uuid");
                                pid = String .valueOf(uuid);
                                break;
                            }
                        }
                        for (JSONObject jsonObject : iterable) {
                            jsonObject.put("pid", pid);
                            Object date1 = jsonObject.get("date");
                            date = String.valueOf(date1);
                            collector.collect(jsonObject);
                            count++;
                        }
                        jsonone.put("date", date);
                        jsonone.put("size",count);
                        collector.collect(jsonone);
                    }
                });
        //单独变形
        apply.addSink(new ckSinkB() );
        apply.addSink(new ckSinkA1());
        //单独伪装
        camouflageapply.addSink(new ckSinkC());
        camouflageapply.addSink(new ckSinkA1());

        env.execute("数据归并");
    }

    private static class ckSinkA extends RichSinkFunction<JSONObject> {
        String sql= "";
        private Statement stmt;
        private  Connection conn;
        //private PreparedStatement preparedStatement;
        String jdbcUrl = "jdbc:clickhouse://192.168.88.161:8123/default";//39.96.136.60:8123,,10.10.41.242:8123,10.10.41.251:8123
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
            value.put("size","1");
            String sql= "";
            StringBuilder columns = new StringBuilder("uuid,date,currenttime,messageProtocolVersion,messageDeviceTypeId,messageProductId,messageDeviceDescribe,messageEmbeddedSoftwareVersion," +
                    "messageChipVersion,messageDeviceSerialId,messagePackageId,messageLoadLength,messageNumber,messageSplitSerialId," +
                    "verifyCode,reserved,size,");
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
            values.append (value.getOrDefault("size","1")).append("','");
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
                        if (s.equals(null)){
                            s = "0";
                        }
                        values.append(s).append("','");
                    }
                    columns.append("tempVerifyCode,messageSplit");
                    values.append (value.getOrDefault("tempVerifyCode","-1")).append("','");
                    values.append (value.getOrDefault("messageSplit","-1")).append("'");
                    String sqlkey = columns.toString();
                    String sqlvalue = values.toString();
                    sql = "INSERT INTO default.bus_warning ( "+ sqlkey+" ) VALUES ( "+sqlvalue +" )";
                    System.out.println("A表");
                    stmt.executeQuery(sql);
                }
            }catch (Exception e){
                System.out.println(e);
            }
        }
    }

    private static class ckSinkB extends RichSinkFunction<JSONObject> {
    String sql= "";
    private Statement stmt;
    private  Connection conn;
    private PreparedStatement preparedStatement;
    String jdbcUrl = "jdbc:clickhouse://192.168.88.161:8123/default";//39.96.136.60:8123,,10.10.41.242:8123,10.10.41.251:8123
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
        //System.out.println(size);
        if (size != null){
            return;
        }
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
                sql = "INSERT INTO default.bus_twarning ( "+ sqlkey+" ) VALUES ( "+sqlvalue +" )";
                System.out.println("单独变星表bus_twarning_local");
                stmt.executeQuery(sql);
            }
        }catch (Exception e){
            System.out.println(e);
        }
    }
}

    private static class ckSinkA1 extends RichSinkFunction<JSONObject> {
        String sql= "";
        private Statement stmt;
        private  Connection conn;
        private PreparedStatement preparedStatement;
        String jdbcUrl = "jdbc:clickhouse://192.168.88.161:8123/default";//39.96.136.60:8123,,10.10.41.242:8123,10.10.41.251:8123
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
            //System.out.println("size");
            if (size == null){
                return;
            }
            String sql= "";
            StringBuilder columns = new StringBuilder("uuid,date,currenttime,messageProtocolVersion,messageDeviceTypeId,messageProductId,messageDeviceDescribe,messageEmbeddedSoftwareVersion," +
                    "messageChipVersion,messageDeviceSerialId,messagePackageId,messageLoadLength,messageNumber,messageSplitSerialId," +
                    "verifyCode,reserved,size,");
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
            values.append (value.getOrDefault("size","-1")).append("','");
            //values.append (value.getOrDefault("pid","-1")).append("','");
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
                        if (s.equals(null)){
                            s = "0";
                        }
                        values.append(s).append("','");
                    }
                    columns.append("tempVerifyCode,messageSplit");
                    values.append (value.getOrDefault("tempVerifyCode","-1")).append("','");
                    values.append (value.getOrDefault("messageSplit","-1")).append("'");
                    String sqlkey = columns.toString();
                    String sqlvalue = values.toString();
                    sql = "INSERT INTO default.bus_warning ( "+ sqlkey+" ) VALUES ( "+sqlvalue +" )";
                    System.out.println("归并sink到总表"+sql);
                    stmt.executeQuery(sql);
                }
            }catch (Exception e){
                System.out.println(e);
            }
        }
    }

    private static class ckSinkC extends RichSinkFunction<JSONObject> {
        String sql= "";
        private Statement stmt;
        private  Connection conn;
        private PreparedStatement preparedStatement;
        String jdbcUrl = "jdbc:clickhouse://192.168.88.161:8123/default";//39.96.136.60:8123,,10.10.41.242:8123,10.10.41.251:8123
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
            //System.out.println(size);
            if (size != null){
                return;
            }
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
                    //default.bus_Camouflage_warning_local
                    sql = "INSERT INTO default.bus_cwarning ( "+ sqlkey+" ) VALUES ( "+sqlvalue +" )";
                    System.out.println("单独伪表bus_cwarning");
                    stmt.executeQuery(sql);
                }
            }catch (Exception e){
                System.out.println(e);
            }
        }
    }
}
