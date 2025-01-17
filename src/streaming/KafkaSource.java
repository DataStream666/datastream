package streaming;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class KafkaSource<T> implements SourceFunction<T> {
    @Override
    public void run(SourceContext<T> ctx) throws Exception {
        // 模拟从 Kafka 中读取数据
        // ctx.collect("Some Kafka data");
    }

    @Override
    public void cancel() {
        // 关闭 Kafka 连接
    }
}


