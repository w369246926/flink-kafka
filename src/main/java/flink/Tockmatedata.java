package flink;

import cn.hutool.core.util.IdUtil;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.hadoop2.org.apache.http.client.config.RequestConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import ru.yandex.clickhouse.BalancedClickhouseDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class Tockmatedata {
    public static void main(String[] args) throws Exception {
        //TODO 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        RequestConfig.custom().setConnectionRequestTimeout(120 * 1000)
                .setSocketTimeout(120 * 1000).setConnectTimeout(120 * 1000).build();


        //TODO 1.source
        //准备kafka连接参数
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "10.10.41.242:9092");//集群地址
        props.setProperty("bootstrap.servers", "10.10.41.243:9092");//集群地址
        props.setProperty("bootstrap.servers", "10.10.41.251:9092");//集群地址
        props.setProperty("group.id", "flink1540");//消费者组id
        props.setProperty("auto.offset.reset", "latest");//latest有offset记录从记录位置开始消费,没有记录从最新的/最后的消息开始消费 /earliest有offset记录从记录位置开始消费,没有记录从最早的/最开始的消息开始消费
        //使用连接参数创建FlinkKafkaConsumer/kafkaSource
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<String>("ahmetadata1", new SimpleStringSchema(), props);
        //使用kafkaSource
        DataStream<String> kafkaDS = env.addSource(kafkaSource).setParallelism(3);;
        //TODO 2.transformation
        DataStream<JSONObject> dataStream = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                //有大括号[json]
                try {
                    if (s.substring(0, 1).equals("[")) {
                        //截取需求部分
                        String substring = s.substring(1, (s.length() - 2));
                        //将不规则字符更换为规则字符
                        String replace = substring.replace("[", "\"").replace("]", "\"");
                        System.out.println("yes[]----" + replace);
                        JSONObject jsonObject = JSONObject.parseObject(replace);
                        jsonObject.put("uuid", IdUtil.simpleUUID());
                        long date = System.currentTimeMillis();
                        jsonObject.put("date", date);
                        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//可以方便地修改日期格式
                        String currenttime = dateFormat.format(date);
                        jsonObject.put("currenttime", currenttime);
                        collector.collect(jsonObject);
                    } else {
                        //无大括号json
                        //System.out.println("no[]----" + s);
                        //将不规则字符更换为规则字符
                        String replace = s.replace("\\\\", "");
                        JSONObject jsonObject = JSONObject.parseObject(replace);
                        jsonObject.put("uuid", IdUtil.simpleUUID());
                        collector.collect(jsonObject);
                    }
                }catch (Exception e){
                    System.out.println(e);
                }
            }
        });


        //TODO 3.sink


        //dataStream.print();
        dataStream.addSink(new ckSink()).setParallelism(1);


        env.execute("安恒原始日志数据");

    }

    private static class ckSink extends RichSinkFunction<JSONObject> {
        String key = "date,uuid,device_id,device_ip,interface_icon,data_type,time,sip,sipv6,smac,sport," +
                "dip,dipv6,dmac,dport,network_protocol,transport_protocol,session_protocol,app_protocol," +
                "sess_id,log_type,mail_time,from,to,cc,subject,bcc,returnpath,received,send_server_domain," +
                "send_server_ip,content_encoding,content_length,mail_size,file_list,client_total_byte," +
                "client_total_pkt,server_total_byte,server_total_pkt,flow_start_time,flow_end_time," +
                "flow_duration,total_byte,avg_pkt_byte,total_pkt,avg_pkt_num,avg_pkt_size,close_status," +
                "avg_delay_time,retrans_pkt_num,version,session_id,server_name,issuer_name,common_name," +
                "not_before,not_after,public_key,c_cipher_suite,s_cipher_suite,ja3,ja3s,method,uri,origin," +
                "cookie,agent,referer,http_req_header,http_res_code,content_type,content_length_tow," +
                "http_res_heade,dns_type,host,mx,cname,res_code_one,count,dns_query_type,aa,tc,rd,ra," +
                "rep_rr_info,aa_rr_info,append_rr_info,rr_ttl,dns_req_len,dns_res_len,dns_req_rr_type," +
                "dns_res_rr_type,dns_res_ip,version_two,username,dbname,req_cmd,res_code,file_dir," +
                "file_name,file_type,file_size,file_md5,currenttime" ;
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
            if (conn != null) {
                conn.close();
            }
        }
        @Override
        public void invoke(JSONObject value, Context context) throws Exception {
            try{

                    String[] splitkey = key.split(",");
                    //获取所有告警的JSON
                    StringBuilder columns = new StringBuilder();
                    StringBuilder values = new StringBuilder();
                    values.append("'");
                for (int i = 0; i < splitkey.length; i++) {

                    if (i == splitkey.length-1){
                        //System.out.println(splitkey.length);
                        values.append(value.getOrDefault(splitkey[i], "-1")).append("'");
                    }else {
                        values.append(value.getOrDefault(splitkey[i], "-1")).append("','");
                    }
                }
                    String sqlvalue = values.toString();
                    sql = "INSERT INTO default.bus_ahmetadata_local ( "+ key+" ) VALUES ( "+sqlvalue +" )";
                System.out.println(sql);
                    stmt.executeQuery(sql);

            }catch (Exception e){
                System.out.println(e);
            }
        }
    }

}
