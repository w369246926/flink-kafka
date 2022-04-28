package flink.test;

import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.errors.MinioException;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * Author itcast
 * Desc 演示DataStream-Sink-基于控制台和文件
 */
public class Sinkminio {
    public static void main(String[] args) throws Exception {
        //TODO 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //TODO 1.source
        DataStream<String> ds = env.readTextFile("data/input/words.txt");

        //TODO 2.transformation
        //TODO 3.sink
        ds.print();
        DataStreamSink<String> stringDataStreamSink = ds.writeAsText("data/output/test", FileSystem.WriteMode.OVERWRITE);
        ds.addSink(new minioSink());
        //TODO 4.execute
        env.execute();
    }
    private static class minioSink implements SinkFunction {
        @Override
        public void invoke(Object value, Context context) throws Exception {

            //InputStream in = value.getInputStream();
            try {
// Create a minioClient with the MinIO server playground, its access keyand secret key.
                MinioClient minioClient =
                        MinioClient.builder()
                                .endpoint("http://10.10.41.251:9090")
                                .credentials("adminminio", "admin123456")
                                .build();
// 创建bucket
                String bucketName = "flinkstreamfilesink";
                boolean exists =
                        minioClient.bucketExists(BucketExistsArgs.builder().bucket(bucketName).build());
                if (!exists) {
// 不存在，创建bucket
                    minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
                }
// 上传文件
                long l = System.currentTimeMillis();
                String name = String.valueOf(l);
//                minioClient.putObject(PutObjectArgs.builder().bucket(bucketName).object(name)
//                        .stream(stream, -1, 10485760).build());
                System.out.println("上传文件成功");
            } catch (MinioException e) {
                System.out.println("Error occurred: " + e);
                System.out.println("HTTP trace: " + e.httpTrace());
            }

        }
    }

}
