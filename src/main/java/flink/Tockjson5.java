package flink;

import cn.hutool.core.util.IdUtil;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
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
import java.util.*;

public class Tockjson5 {
    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String second = "10";
        if (parameterTool.has("second")) {
            second = parameterTool.get("second");
            System.out.println("指定了归并时间:" + second +"秒");
        } else {
            second = "10";
            System.out.println("设置指定归并时间使用 --second ,没有指定使用默认的:" + second +"秒");
        }
        long wmin = Long.parseLong(second);

        //TODO 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //TODO 1.source
        //准备kafka连接参数
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "10.10.41.242:9092");//集群地址
        props.setProperty("bootstrap.servers", "10.10.41.243:9092");//集群地址
        props.setProperty("bootstrap.servers", "10.10.41.251:9092");//集群地址
        props.setProperty("group.id", "flink");//消费者组id
        props.setProperty("auto.offset.reset", "latest");//latest有offset记录从记录位置开始消费,没有记录从最新的/最后的消息开始消费 /earliest有offset记录从记录位置开始消费,没有记录从最早的/最开始的消息开始消费
        //使用连接参数创建FlinkKafkaConsumer/kafkaSource
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<String>("da_trace", new SimpleStringSchema(), props);
        //使用kafkaSource
        DataStream<String> kafkaDS = env.addSource(kafkaSource);
        //TODO 2.transformation

        //1,将kafka,String数据变更成jsonobj并打上时间戳time和UUID
        DataStream<JSONObject> dataStream = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {

            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    JSONArray basicMessageBasicList = new JSONArray();
                //获取所有告警的JSON
                    JSONArray basicMessageBasicListorg = jsonObject.getJSONArray("basicMessageBasicList");
                System.out.println(wmin);
                    Map map = new HashMap();
                //遍历告警JSON
                    for (int i = 0; i < basicMessageBasicListorg.size(); i++) {
                        map = basicMessageBasicListorg.getJSONObject(i);
                        basicMessageBasicList.add(map);
                        jsonObject.put("basicMessageBasicList",basicMessageBasicList);
                        jsonObject.put("uuid", IdUtil.simpleUUID());
                        long date = new Date().getTime();
                        jsonObject.put("date",date);
                        out.collect(jsonObject);
                        basicMessageBasicList.clear();

                    }

            }
        });
        //2,将数据拆分成两个流,1:伪装和标签,2:变形
        OutputTag<JSONObject> camouflageandlabel = new OutputTag<JSONObject>("伪装和标签", TypeInformation.of(JSONObject.class));
        OutputTag<JSONObject> transformation = new OutputTag<JSONObject>("变形",TypeInformation.of(JSONObject.class)){};
        SingleOutputStreamOperator<JSONObject> process = dataStream.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject jsonObject, Context context, Collector<JSONObject> collector) throws Exception {
                //获取到数据体basicMessageBasicList
                JSONArray basicMessageBasicList = jsonObject.getJSONArray("basicMessageBasicList");
                Map map = new HashMap();
                //遍历告警JSON
                for (int i = 0; i < basicMessageBasicList.size(); i++) {
                    map = basicMessageBasicList.getJSONObject(i);
                    Object verifyFunctionModuleCode = map.get("verifyFunctionModuleCode");
                    //System.out.println(verifyFunctionModuleCode);
                    if (map.get("verifyFunctionModuleCode").equals(6)) {
                        map.put("verifyFunctionModuleCode", "变形");
                        if (map.get("verifyTypeId").equals(1))
                            map.put("verifyTypeId", "标签目的地址未在白名单告警");
                        else if (map.get("verifyTypeId").equals(2))
                            map.put("verifyTypeId", "非法访问跳变地址告警");
                        else if (map.get("verifyTypeId").equals(5))
                            map.put("verifyTypeId", "变形会话异常结束告警");
                        else if (map.get("verifyTypeId").equals(8))
                            map.put("verifyTypeId", "变形会话");
                        else if (map.get("verifyTypeId").equals(7))
                            map.put("verifyTypeId", "变形会话异常结束告警");
                        else if (map.get("verifyTypeId").equals(0))
                            map.put("verifyTypeId", "标签校验错误告警");
                        else map.put("verifyTypeId", "模拟");
                        context.output(transformation, jsonObject);
                    }
                    else if (map.get("verifyFunctionModuleCode").equals(4)) {
                        map.put("verifyFunctionModuleCode", "隐身");
                        if (map.get("verifyTypeId").equals(1))
                            map.put("verifyTypeId", "隐身ARP告警");
                        else if (map.get("verifyTypeId").equals(0))
                            map.put("verifyTypeId", "非法访问告警");
                        else map.put("verifyTypeId", "模拟");
                        context.output(camouflageandlabel, jsonObject);
                    }
                    else if (map.get("verifyFunctionModuleCode").equals(5)) {
                        map.put("verifyFunctionModuleCode", "伪装");
                        if (map.get("verifyTypeId").equals(1))
                            map.put("verifyTypeId", "攻击会话开始告警");
                        else if (map.get("verifyTypeId").equals(2))
                            map.put("verifyTypeId", "攻击会话正常结束告警");
                        else if (map.get("verifyTypeId").equals(3))
                            map.put("verifyTypeId", "攻击会话异常结束告警");
                        else if (map.get("verifyTypeId").equals(4))
                            map.put("verifyTypeId", "回应主机主动向外发包告警");
                        else if (map.get("verifyTypeId").equals(5))
                            map.put("verifyTypeId", "ICMP报文攻击告警");
                        else if (map.get("verifyTypeId").equals(7))
                            map.put("verifyTypeId", "回应ICMP包告警");
                        else if (map.get("verifyTypeId").equals(8))
                            map.put("verifyTypeId", "回应主机主动向外发送ICMP包告警");
                        else if (map.get("verifyTypeId").equals(9))
                            map.put("verifyTypeId", "TCP会话超时告警");
                        else if (map.get("verifyTypeId").equals(10))
                            map.put("verifyTypeId", "UDP会话超时告警");
                        else if (map.get("verifyTypeId").equals(11))
                            map.put("verifyTypeId", "ICMP会话超时告警");
                        else if (map.get("verifyTypeId").equals(12))
                            map.put("verifyTypeId", "TCP会话长连接告警");
                        else if (map.get("verifyTypeId").equals(13))
                            map.put("verifyTypeId", "5.5.2 UDP会话长连接告警");
                        else if (map.get("verifyTypeId").equals(14))
                            map.put("verifyTypeId", "ARP广播告警");
                        else if (map.get("verifyTypeId").equals(15))
                            map.put("verifyTypeId", "回应ARP广播告警");
                        else if (map.get("verifyTypeId").equals(0))
                            map.put("verifyTypeId", "扫描未伪装端口告警");
                        else map.put("verifyTypeId", "模拟");
                        context.output(camouflageandlabel, jsonObject);
                    }
                    else {
                        map.put("verifyFunctionModuleCode", "标签");
                        if (map.get("verifyTypeId").equals(1))
                            map.put("verifyTypeId", "安全标签告警");
                        else if (map.get("verifyTypeId").equals(2))
                            map.put("verifyTypeId", "标签错误告警");
                        else map.put("verifyTypeId", "模拟");
                        context.output(camouflageandlabel, jsonObject);
                    }

                }
            }
        });
        //2,将数据拆分成两个流,1:伪装和标签,2:变形
        DataStream<JSONObject> camouflageandlabel1 = process.getSideOutput(camouflageandlabel);
        DataStream<JSONObject> transformation2 = process.getSideOutput(transformation);

        //3,伪装和标签直接存储到A表,结束
        camouflageandlabel1.addSink(new ckSinkA());

        //5,变形进行按照时间归并:5分钟归并一次,取时间最大值,并存储到A表
        SingleOutputStreamOperator<JSONObject> apply = transformation2.keyBy(t -> {
            Map map = new HashMap();
            Object verifyTypeId = null;
            JSONArray basicMessageBasicList = t.getJSONArray("basicMessageBasicList");
            for (int i = 0; i < basicMessageBasicList.size(); i++) {
                map = basicMessageBasicList.getJSONObject(i);
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

        apply.addSink(new ckSinkB());
        apply.addSink(new ckSinkA1());



        //reduce.print();
        //reduce.addSink(new ckSinkA1());

        env.execute("数据归并");
    }

    private static class ckSinkA extends RichSinkFunction<JSONObject> {
        String sql= "";
        private Statement stmt;
        private  Connection conn;
        private PreparedStatement preparedStatement;
        String jdbcUrl = "jdbc:clickhouse://10.10.41.251:8123/default";//39.96.136.60:8123,,10.10.41.242:8123,10.10.41.251:8123
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
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }
        @Override
        public void invoke(JSONObject value, Context context) throws Exception {
            String sql= "";
            StringBuilder columns = new StringBuilder("uuid,date,messageProtocolVersion,messageDeviceTypeId,messageProductId,messageDeviceDescribe,messageEmbeddedSoftwareVersion," +
                    "messageChipVersion,messageDeviceSerialId,messagePackageId,messageLoadLength,messageNumber,messageSplitSerialId," +
                    "verifyCode,reserved,");
            StringBuilder values = new StringBuilder();
            values.append("'");
            values.append (value.getOrDefault("uuid","-1")).append("','");
            values.append (value.getOrDefault("date","-1")).append("','");
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
                    sql = "INSERT INTO default.report ( "+ sqlkey+" ) VALUES ( "+sqlvalue +" )";
                    //System.out.println("A表");
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
    String jdbcUrl = "jdbc:clickhouse://10.10.41.251:8123/default";//39.96.136.60:8123,,10.10.41.242:8123,10.10.41.251:8123
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
        if (preparedStatement != null) {
            preparedStatement.close();
        }
    }
    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        Object size = value.get("size");
        System.out.println(size);
        if (size != null){
            return;
        }
        String sql= "";
        StringBuilder columns = new StringBuilder("uuid,date,messageProtocolVersion,messageDeviceTypeId,messageProductId,messageDeviceDescribe,messageEmbeddedSoftwareVersion," +
                "messageChipVersion,messageDeviceSerialId,messagePackageId,messageLoadLength,messageNumber,messageSplitSerialId," +
                "verifyCode,reserved,pid,");
        StringBuilder values = new StringBuilder();
        values.append("'");
        values.append (value.getOrDefault("uuid","-1")).append("','");
        values.append (value.getOrDefault("date","-1")).append("','");
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
                sql = "INSERT INTO default.report_deformation ( "+ sqlkey+" ) VALUES ( "+sqlvalue +" )";
                //System.out.println("B表");
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
        String jdbcUrl = "jdbc:clickhouse://10.10.41.251:8123/default";//39.96.136.60:8123,,10.10.41.242:8123,10.10.41.251:8123
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
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }
        @Override
        public void invoke(JSONObject value, Context context) throws Exception {
            Object size = value.get("size");
            System.out.println("size");
            if (size == null){
                return;
            }
            String sql= "";
            StringBuilder columns = new StringBuilder("uuid,date,messageProtocolVersion,messageDeviceTypeId,messageProductId,messageDeviceDescribe,messageEmbeddedSoftwareVersion," +
                    "messageChipVersion,messageDeviceSerialId,messagePackageId,messageLoadLength,messageNumber,messageSplitSerialId," +
                    "verifyCode,reserved,size,");
            StringBuilder values = new StringBuilder();
            values.append("'");
            values.append (value.getOrDefault("uuid","-1")).append("','");
            values.append (value.getOrDefault("date","-1")).append("','");
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
                        values.append(s).append("','");
                    }
                    columns.append("tempVerifyCode,messageSplit");
                    values.append (value.getOrDefault("tempVerifyCode","-1")).append("','");
                    values.append (value.getOrDefault("messageSplit","-1")).append("'");
                    String sqlkey = columns.toString();
                    String sqlvalue = values.toString();
                    sql = "INSERT INTO default.report ( "+ sqlkey+" ) VALUES ( "+sqlvalue +" )";
                    //System.out.println("A1表"+sql);
                    stmt.executeQuery(sql);
                }
            }catch (Exception e){
                System.out.println(e);
            }
        }
    }
}
