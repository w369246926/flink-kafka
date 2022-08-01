package flink.dawarning.transformation;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class WindowApply implements WindowFunction<JSONObject, JSONObject, Object, TimeWindow> {
    @Override
    public void apply(Object o, TimeWindow timeWindow, Iterable<JSONObject> iterable, Collector<JSONObject> collector) throws Exception {
        String pid = "";
        int count = 0;
        JSONObject jsonone = null;
        String date="";
        for (JSONObject jsonObject : iterable) {
            if (count == 0) {
                jsonone = jsonObject;
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
}
