package flink.dawarning.source;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import flink.dawarning.sink.CWarningSink;
import flink.dawarning.sink.TWarningSink;
import flink.dawarning.sink.WarningSink;
import flink.dawarning.transformation.WarningFlatMap;

import flink.dawarning.transformation.WindowApply;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Map;
import java.util.Properties;

public class DaWarning {
    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String second = "15";
        if (parameterTool.has("second")) {
            second = parameterTool.get("second");
            System.out.println("未指定了归并时间,默认:" + second +"秒");
        } else {
            second = "120";
            System.out.println("设置指定归并时间使用 --second ,没有指定使用默认的:" + second +"秒");
        }
        long wmin = Long.parseLong(second);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        //1.1 开启CheckPoint
        //env.enableCheckpointing(5 * 60000L, CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setCheckpointTimeout(10 * 60000L);
        //env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        //env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000L));

        //1.2 设置状态后端
        //env.setStateBackend(new HashMapStateBackend());
        //env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/211126/ck");
        //System.setProperty("HADOOP_USER_NAME", "atguigu");
        //TODO:1使用自定义kafkaSource
        String topic = "da_warning";
        String groupid = "flink";
        DataStreamSource<String> stringDataStreamSource = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupid));

        DataStream<JSONObject> dataStream = stringDataStreamSource.flatMap(new WarningFlatMap());

//2,将数据拆分成两个流,1:伪装和标签,2:变形
        OutputTag<JSONObject> camouflages = new OutputTag<JSONObject>("伪装和标签", TypeInformation.of(JSONObject.class));
        OutputTag<JSONObject> camouflageandlabel = new OutputTag<JSONObject>("标签", TypeInformation.of(JSONObject.class));
        OutputTag<JSONObject> transformation = new OutputTag<JSONObject>("变形",TypeInformation.of(JSONObject.class)){};
        //SingleOutputStreamOperator<JSONObject> process = dataStream.process(new WarningProcess());
        SingleOutputStreamOperator<JSONObject> process = dataStream.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject jsonObject, Context context, Collector<JSONObject> collector) throws Exception {
                //获取到数据体basicMessageBasicList
                JSONArray basicMessageBasicList = jsonObject.getJSONArray("basicMessageBasicList");
                for (int i = 0; i < basicMessageBasicList.size(); i++) {
                    Map map1 = basicMessageBasicList.getJSONObject(i);
                    Object verifyFunctionModuleCode = map1.get("verifyFunctionModuleCode");
                    //System.out.println(verifyFunctionModuleCode);
                    if (map1.get("verifyFunctionModuleCode").equals(6)) {
                        map1.put("verifyFunctionModuleCode", "网络变形");
                        if (map1.get("verifyTypeId").equals(1)) {
                            map1.put("verifyTypeId", "标签目的地址未在白名单告警");
                        } else if (map1.get("verifyTypeId").equals(2)) {
                            map1.put("verifyTypeId", "非法访问跳变地址告警");
                        } else if (map1.get("verifyTypeId").equals(5)) {
                            map1.put("verifyTypeId", "变形会话异常结束告警");
                        } else if (map1.get("verifyTypeId").equals(8)) {
                            map1.put("verifyTypeId", "变形会话");
                        } else if (map1.get("verifyTypeId").equals(7)) {
                            map1.put("verifyTypeId", "变形会话异常结束告警");
                        } else if (map1.get("verifyTypeId").equals(0)) {
                            map1.put("verifyTypeId", "标签校验错误告警");
                        } else {
                            map1.put("verifyTypeId", "模拟");
                        }
                        context.output(transformation, jsonObject);
                    }
                    else if (map1.get("verifyFunctionModuleCode").equals(4)) {
                        map1.put("verifyFunctionModuleCode", "网络隐身");
                        if (map1.get("verifyTypeId").equals(1)) {
                            map1.put("verifyTypeId", "隐身ARP告警");
                        } else if (map1.get("verifyTypeId").equals(0)) {
                            map1.put("verifyTypeId", "非法访问告警");
                        } else {
                            map1.put("verifyTypeId", "模拟");
                        }
                        context.output(camouflageandlabel, jsonObject);
                    }
                    else if (map1.get("verifyFunctionModuleCode").equals(5)) {
                        //map1.put("verifyFunctionModuleCode", "网络伪装");
                        if (map1.get("verifyTypeId").equals(1)) {
                            map1.put("verifyFunctionModuleCode", "攻陷回应主机事件");
                            map1.put("verifyTypeId", "攻击会话建立事件");
                            Object srcIp = map1.get("srcIp");
                            Object destIp = map1.get("destIp");
                            map1.put("incidentdescription", ""+srcIp+"与"+destIp+"伪装IP建立了TCP/UDP攻击会话");
                        } else if (map1.get("verifyTypeId").equals(2)) {
                            map1.put("verifyFunctionModuleCode", "攻陷回应主机事件");
                            map1.put("verifyTypeId", "攻击会话结束事件");
                            Object srcIp = map1.get("srcIp");
                            Object destIp = map1.get("destIp");
                            map1.put("incidentdescription", ""+srcIp+"与"+destIp+"伪装IP的TCP攻击会话结束");
                        } else if (map1.get("verifyTypeId").equals(3)) {
                            map1.put("verifyFunctionModuleCode", "网络扫描事件");
                            map1.put("verifyTypeId", "IP扫描事件");
                            Object srcIp = map1.get("srcIp");
                            Object destIp = map1.get("destIp");
                            map1.put("incidentdescription", ""+srcIp+"与"+destIp+"伪装IP的TCP攻击会话结束");
                        } else if (map1.get("verifyTypeId").equals(4)) {
                            map1.put("verifyFunctionModuleCode", "跳板攻击事件");
                            map1.put("verifyTypeId", "基于tcp/udp的横向探测事件");
                            Object messageDeviceSerialId = jsonObject.get("messageDeviceSerialId");
                            map1.put("incidentdescription", ""+messageDeviceSerialId+"主动发动TCP/UDP请求");
                        } else if (map1.get("verifyTypeId").equals(5)) {
                            map1.put("verifyFunctionModuleCode", "网络扫描事件");
                            map1.put("verifyTypeId", "IP扫描事件");
                            Object srcIp = map1.get("srcIp");
                            Object destIp = map1.get("destIp");
                            map1.put("incidentdescription", ""+srcIp+"对"+destIp+"伪装地址进行PING攻击");
                        } else if (map1.get("verifyTypeId").equals(7)) {
                            map1.put("verifyFunctionModuleCode", "网络扫描事件");
                            map1.put("verifyTypeId", "端口扫描事件");
                            Object srcIp = map1.get("srcIp");
                            Object destIp = map1.get("destIp");
                            map1.put("incidentdescription", ""+srcIp+"与"+destIp+"伪装IP的TCP/UDP/ICMP攻击会话超时");
                        } else if (map1.get("verifyTypeId").equals(8)) {
                            map1.put("verifyFunctionModuleCode", "攻陷回应主机事件");
                            map1.put("verifyTypeId", "回攻击会话持续事件");
                            //map1.put("verifyTypeId", "持续入侵事件");
                            Object srcIp = map1.get("srcIp");
                            Object destIp = map1.get("destIp");
                            map1.put("incidentdescription", ""+srcIp+"与"+destIp+"伪装IP的TCP/UDP会话未结束");
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
                            map1.put("verifyFunctionModuleCode", "网络扫描事件");
                            map1.put("verifyTypeId", "端口扫描事件");
                            Object srcIp = map1.get("srcIp");
                            Object destIp = map1.get("destIp");
                            map1.put("incidentdescription", ""+srcIp+" 扫描未伪装的"+destIp+"服务端口");
                        }else if (map1.get("verifyTypeId").equals(6)) {
                            map1.put("verifyFunctionModuleCode", "跳板攻击事件");
                            map1.put("verifyTypeId", "基于ICMP的横向探测事件");
                            Object messageDeviceSerialId = jsonObject.get("messageDeviceSerialId");
                            map1.put("incidentdescription", ""+messageDeviceSerialId+"回应主机主动发送ICMP请求");
                        }else {
                            map1.put("verifyTypeId", "模拟");
                        }
                        context.output(camouflages, jsonObject);
                    }
                    else if (map1.get("verifyFunctionModuleCode").equals(10)){
                        //map1.put("verifyFunctionModuleCode", "网络伪装");
                        if (map1.get("verifyTypeId").equals(2)) {
                            map1.put("verifyFunctionModuleCode", "网络扫描事件");
                            map1.put("verifyTypeId", "IP扫描事件");
                            Object srcIp = map1.get("srcIp");
                            Object destIp = map1.get("destIp");
                            map1.put("incidentdescription", ""+srcIp+"对"+destIp+"伪装地址进行ARP请求");
                        } else if (map1.get("verifyTypeId").equals(3)) {
                            map1.put("verifyFunctionModuleCode", "跳板攻击事件");
                            map1.put("verifyTypeId", "基于ARP的横向探测事件");
                            //map1.put("verifyTypeId", "疑似横向移动");
                            Object messageDeviceSerialId = jsonObject.get("messageDeviceSerialId");
                            map1.put("incidentdescription", ""+messageDeviceSerialId+"回应主机主动发送ARP请求");
                        } else if (map1.get("verifyTypeId").equals(4)) {
                            map1.put("verifyTypeId", "IP冲突");
                            Object srcIp = map1.get("srcIp");
                            Object destIp = map1.get("destIp");
                            map1.put("incidentdescription", ""+destIp+"伪装IP与网内其他IP冲突");
                        }
                        else {
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
        //2,将数据拆分成3个流,1:归并后全部,2:伪装,3:变形.
        DataStream<JSONObject> warning = process.getSideOutput(camouflageandlabel);
        DataStream<JSONObject> camouflage = process.getSideOutput(camouflages);
        DataStream<JSONObject> transformation2 = process.getSideOutput(transformation);

        //5,变形进行按照时间归并:5分钟归并一次,取时间最大值,并存储到A表
        SingleOutputStreamOperator<JSONObject> tapply = transformation2.keyBy(t -> {
                    Object verifyTypeId = null;
                    JSONArray basicMessageBasicList = t.getJSONArray("basicMessageBasicList");
                    for (int i = 0; i < basicMessageBasicList.size(); i++) {
                        Map map = basicMessageBasicList.getJSONObject(i);
                        verifyTypeId = map.get("verifyTypeId");
                    }
                    return verifyTypeId;
                })
                .window(TumblingProcessingTimeWindows.of(Time.seconds(wmin)))
                .apply(new WindowApply());
        //6,伪装进行按照时间归并:5分钟归并一次,取时间最大值,并存储到A表
        SingleOutputStreamOperator<JSONObject> capply = camouflage.keyBy(t -> {
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
                .apply(new WindowApply());
        //存储到归并后的告警表
        warning.addSink(new WarningSink());
        //存储到变形告警表和归并告警表
        tapply.addSink(new TWarningSink());
        //存储到伪装告警表和归并告警表
        capply.addSink(new CWarningSink());

        env.execute("数据归并");

    }
}
