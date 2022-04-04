package flink.test;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;



import java.io.IOException;
import java.util.Map;

public class Jobname {
        static String jarId = "c00a8e45-7180-4eed-af14-f30f2102fb86_tock1.12.4-1.0-SNAPSHOT.jar";
        static String jobName = "数据归并";
        static String urlPrefix = "http://10.10.41.251:8081";

        public static void main(String[] args) throws IOException, InterruptedException {
            String jobID = getJobID(urlPrefix, jobName);
            System.out.println(jobID);
            String jobState = getJobState(urlPrefix, jobID);
            System.out.println(jobState);
            if ("RUNNING".equals(jobState)) {
                Thread.sleep(3000);
                int stop = stop(urlPrefix, jobID);
                if (stop == 202){
                    System.out.println("关闭程序,并执行重启");
                }else{
                    System.out.println("未能关闭进程");
                    return;
                }
            }
            //执行新的进程
            start(urlPrefix, jarId);
            //System.out.println(start);
//            try {
//                    getJobState(urlPrefix, jobID);
//            } catch (IOException e) {
//                e.printStackTrace();
//            } finally {
//                System.out.println("pipeline stop");
//            }


        }

    private static String start(String urlPrefix, String jarId) {
        String baseUrl = "http://host:port/jars/${jarId}/run";
        Map<String, String> params = new HashMap<>();
        params.put("programArgs", "xxxxxx");
        params.put("entryClass", "com.xx.oo.JsonMain");
        params.put("parallelism", "2");
        params.put("savepointPath", null);
        Request request = new Request.Builder()
                .url(baseUrl)
                //.addHeader(userAgent, userAgentVal)
                .post(RequestBody.create(JSON.toJSONString(params), MEDIA_TYPE_JSON))
                .build();
        Response resp = OkHttpUtils.execute(request);
        String respBody = resp.body().string();
        if (OK == resp.code()) {
            JSONObject body = JSON.parseObject(respBody);
            return body.getString("jobid");
        }
        return null;
    }

    /*若任务的状态为running，则调用api结束任务*/
        public static int stop(String urlPrefix, String jid) throws IOException {
            CloseableHttpResponse stopJob = send(new HttpPatch(urlPrefix + "/jobs/" + jid));
            StatusLine statusLine = stopJob.getStatusLine();
            int statusCode = statusLine.getStatusCode();
            /*202代表执行成功，关闭pipeline*/
            if (statusCode != 202) {
                throw new RuntimeException("当前任务未能正确关闭");
            }
            return statusCode;

        }

        /*判断当前任务的状态   "state": "RUNNING" */
        public static String getJobState(String urlPrefix, String jid) throws IOException {
            CloseableHttpResponse getJobState = send(new HttpGet(urlPrefix + "/jobs/" + jid));
            String getJobStateRes = transform(getJobState);
            JSONObject jsonObject = JSON.parseObject(getJobStateRes);
            String state = jsonObject.getString("state");
            if (!"RUNNING".equals(state)) {
                throw new RuntimeException("当前任务处于停止或故障状态");
            }
            return state;
        }

        /*根据jobname获取jobid*/
        public static String getJobID(String urlPrefix, String jobName) throws IOException {
            CloseableHttpResponse getJobID = send(new HttpGet(urlPrefix + "/jobs/overview"));
            String getJobIDRes = transform(getJobID);
            JSONObject res = JSON.parseObject(getJobIDRes);
            JSONArray jobs = res.getJSONArray("jobs");
            String jid = null;
            for (int i = 0; i < jobs.size(); i++) {
                JSONObject jsonObject = (JSONObject) jobs.get(i);
                String name = (String) jsonObject.get("name");
                if (name.equals(jobName)) {
                    jid = (String) jsonObject.get("jid");
                    break;
                }
            }
            if (jid == null || jid.equals("")) {
                throw new RuntimeException("pipeline在flink中没有对应的任务");
            }
            return jid;
        }

        public static CloseableHttpResponse send(HttpRequestBase httpRequestBase) throws IOException {
            CloseableHttpClient httpclient = HttpClients.createDefault();
            CloseableHttpResponse response = httpclient.execute(httpRequestBase);
            return response;
        }

        public static String transform(CloseableHttpResponse response) throws IOException {
            HttpEntity entity = response.getEntity();
            String result = EntityUtils.toString(entity);
            return result;
        }


    }
