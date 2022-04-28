package flink.test;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;

/**
 * Author itcast
 * Desc 演示DataStream-Sink-基于控制台和文件
 */
public class SinkDemo01 {
    public static void main(String[] args) throws Exception {
        //TODO 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //TODO 1.source
        DataStream<String> ds = env.readTextFile("data/input/words.txt");

        //TODO 2.transformation
        //TODO 3.sink
        ds.print();
        //ds.print("输出标识");
        //ds.printToErr();//会在控制台上以红色输出
        //ds.printToErr("输出标识");//会在控制台上以红色输出
        //ds.writeAsText("data/output/result1").setParallelism(1);
        //ds.writeAsText("data/output/result2").setParallelism(2);
        //ds.writeAsText("s3://10.10.41.251:9090/flinkstreamfilesink/data");
        //使用FileSink将数据sink到HDFS
        /*OutputFileConfig config = OutputFileConfig
                .builder()
                .withPartPrefix("prefix")
                .withPartSuffix(".txt")
                .build();

        FileSink<String> sink = FileSink
                .forRowFormat(new Path("hdfs://10.10.41.242:8020/flinkstreamfilesink/data"), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .build())
                .withOutputFileConfig(config)
                .withBucketAssigner(new DateTimeBucketAssigner("yyyy-MM-dd"))
                .build();

        ds.sinkTo(sink);*/

        //TODO 4.execute
        env.execute();
    }
}
