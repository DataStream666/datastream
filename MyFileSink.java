package streaming;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.scala.createTypeInformation;
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.sink.OutputFileConfig;
import org.apache.flink.connector.file.sink.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.connector.file.sink.DefaultRollingPolicy;
import org.apache.flink.core.memory.MemorySize;
import org.apache.flink.streaming.api.watermark.WatermarkStrategy;

import java.time.Duration;
import java.time.ZoneId;

public class MyFileSink {

    public static void main(String[] args) throws Exception {
        // 创建 StreamExecutionEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度
        env.setParallelism(2);

        // 启用检查点
        env.enableCheckpointing(2000);

        // 模拟生成无界数据流
        DataStream<String> dataStream = env.addSource(new SourceFunction<String>() {
            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                long count = 0;
                while (true) {
                    ctx.collect("Generated Value: " + count);
                    count++;
                    Thread.sleep(1000); // 每秒生成一条数据
                }
            }

            @Override
            public void cancel() {
                // 取消操作时的处理
            }
        });

        // 输出到文件系统
        FileSink<String> fileSink = FileSink
                .<String>forRowFormat(new Path("f:/tmp"), new SimpleStringEncoder<>("UTF-8"))
                // 设置输出文件配置，指定文件前缀和后缀
                .withOutputFileConfig(
                        OutputFileConfig.builder()
                                .withPartPrefix("file-")
                                .withPartSuffix(".log")
                                .build()
                )
                // 根据时间进行分桶，按小时分桶
                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd HH", ZoneId.systemDefault()))
                // 设置文件滚动策略：1分钟滚动一次文件
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofMinutes(1))
                                .withMaxPartSize(MemorySize.ofMebiBytes(1)) // 设置最大文件大小为 1MB
                                .build()
                )
                .build();

        // 将数据流输出到文件
        dataStream.sinkTo(fileSink);

        // 启动执行环境
        env.execute("Flink File Sink Example");
    }
}
