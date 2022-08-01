package flink.dawarning.transformation;

import cn.hutool.core.util.IdUtil;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Map;

public class WarningFlatMap implements FlatMapFunction<String, JSONObject>{

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
}
