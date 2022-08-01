//package flink.dawarning.transformation;
//
//import com.alibaba.fastjson.JSONArray;
//import com.alibaba.fastjson.JSONObject;
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.streaming.api.functions.ProcessFunction;
//import org.apache.flink.util.Collector;
//import org.apache.flink.util.OutputTag;
//
//import java.util.Map;
//
//public class WarningProcess extends ProcessFunction<JSONObject, JSONObject> {
//
//    OutputTag<JSONObject> camouflages = new OutputTag<JSONObject>("伪装和标签", TypeInformation.of(JSONObject.class));
//    OutputTag<JSONObject> camouflageandlabel = new OutputTag<JSONObject>("标签", TypeInformation.of(JSONObject.class));
//    OutputTag<JSONObject> transformation = new OutputTag<JSONObject>("变形",TypeInformation.of(JSONObject.class)){};
//
//
//
//    @Override
//    public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
//        //获取到数据体basicMessageBasicList
//        JSONArray basicMessageBasicList = jsonObject.getJSONArray("basicMessageBasicList");
//
//        //遍历告警JSON
//        for (int i = 0; i < basicMessageBasicList.size(); i++) {
//            Map map1 = basicMessageBasicList.getJSONObject(i);
//            Object verifyFunctionModuleCode = map1.get("verifyFunctionModuleCode");
//            //System.out.println(verifyFunctionModuleCode);
//            if (map1.get("verifyFunctionModuleCode").equals(6)) {
//                map1.put("verifyFunctionModuleCode", "网络变形");
//                if (map1.get("verifyTypeId").equals(1))
//                    map1.put("verifyTypeId", "标签目的地址未在白名单告警");
//                else if (map1.get("verifyTypeId").equals(2))
//                    map1.put("verifyTypeId", "非法访问跳变地址告警");
//                else if (map1.get("verifyTypeId").equals(5))
//                    map1.put("verifyTypeId", "变形会话异常结束告警");
//                else if (map1.get("verifyTypeId").equals(8))
//                    map1.put("verifyTypeId", "变形会话");
//                else if (map1.get("verifyTypeId").equals(7))
//                    map1.put("verifyTypeId", "变形会话异常结束告警");
//                else if (map1.get("verifyTypeId").equals(0))
//                    map1.put("verifyTypeId", "标签校验错误告警");
//                else map1.put("verifyTypeId", "模拟");
//                context.output(transformation, jsonObject);
//            }
//            else if (map1.get("verifyFunctionModuleCode").equals(4)) {
//                map1.put("verifyFunctionModuleCode", "网络隐身");
//                if (map1.get("verifyTypeId").equals(1))
//                    map1.put("verifyTypeId", "隐身ARP告警");
//                else if (map1.get("verifyTypeId").equals(0))
//                    map1.put("verifyTypeId", "非法访问告警");
//                else map1.put("verifyTypeId", "模拟");
//                context.output(camouflageandlabel, jsonObject);
//            }
//            else if (map1.get("verifyFunctionModuleCode").equals(5)) {
////                        try {
////                            if (jsonObject.get("messageDeviceSerialId").equals("JAJX01710001C100")) {
////                                continue;
////                            }
////                        }catch (Exception e){
////                            System.out.println(e);
////                        }
//                map1.put("verifyFunctionModuleCode", "网络伪装");
//                if (map1.get("verifyTypeId").equals(1)) {
//                    map1.put("verifyTypeId", "攻击会话开始告警");
//                    map1.put("incidentdescription", "攻击会话建立事件");
//                } else if (map1.get("verifyTypeId").equals(2)) {
//                    map1.put("verifyTypeId", "攻击会话正常结束告警");
//                    map1.put("incidentdescription", "攻击会话结束事件");
//                } else if (map1.get("verifyTypeId").equals(3)) {
//                    map1.put("verifyTypeId", "攻击会话异常结束告警");
//                    map1.put("incidentdescription", "IP扫描事件");
//                } else if (map1.get("verifyTypeId").equals(4)) {
//                    map1.put("verifyTypeId", "回应主机主动向外发包告警");
//                    map1.put("incidentdescription", "基于TCP/UDP的横向探测事件");
//                } else if (map1.get("verifyTypeId").equals(5)) {
//                    map1.put("verifyTypeId", "ICMP报文攻击告警");
//                    map1.put("incidentdescription", "IP扫描事件");
//
//                } else if (map1.get("verifyTypeId").equals(7)) {
//                    map1.put("verifyTypeId", "回应ICMP包告警");
//                    map1.put("incidentdescription", "端口扫描事件");
//                } else if (map1.get("verifyTypeId").equals(8)) {
//                    map1.put("verifyTypeId", "回应主机主动向外发送ICMP包告警");
//                    map1.put("incidentdescription", "回攻击会话持续事件");
//                } else if (map1.get("verifyTypeId").equals(9)) {
//                    map1.put("verifyTypeId", "TCP会话超时告警");
//                } else if (map1.get("verifyTypeId").equals(10)) {
//                    map1.put("verifyTypeId", "UDP会话超时告警");
//                } else if (map1.get("verifyTypeId").equals(11)) {
//                    map1.put("verifyTypeId", "ICMP会话超时告警");
//                } else if (map1.get("verifyTypeId").equals(12)) {
//                    map1.put("verifyTypeId", "TCP会话长连接告警");
//                } else if (map1.get("verifyTypeId").equals(13)) {
//                    map1.put("verifyTypeId", "5.5.2 UDP会话长连接告警");
//                } else if (map1.get("verifyTypeId").equals(14)) {
//                    map1.put("verifyTypeId", "ARP广播告警");
//                } else if (map1.get("verifyTypeId").equals(15)) {
//                    map1.put("verifyTypeId", "回应ARP广播告警");
//                } else if (map1.get("verifyTypeId").equals(0)) {
//                    map1.put("verifyTypeId", "扫描未伪装端口告警");
//                    map1.put("incidentdescription", "端口扫描事件");
//                }else if (map1.get("verifyTypeId").equals(6)) {
//                    map1.put("verifyTypeId", "扫描未伪装端口告警");
//                    map1.put("incidentdescription", "基于ICMP的横向探测事件");
//                }else {
//                    map1.put("verifyTypeId", "模拟");
//                }
//                context.output(camouflages, jsonObject);
//            }
//            else {
//                map1.put("verifyFunctionModuleCode", "安全标签");
//                if (map1.get("verifyTypeId").equals(1)) {
//                    map1.put("verifyTypeId", "安全标签告警");
//                } else if (map1.get("verifyTypeId").equals(2)) {
//                    map1.put("verifyTypeId", "标签错误告警");
//                } else {
//                    map1.put("verifyTypeId", "模拟");
//                }
//                context.output(camouflageandlabel, jsonObject);
//            }
//
//        }
//    }
//}
