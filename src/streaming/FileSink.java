package streaming;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import javax.naming.Context;

public class FileSink<T> implements SinkFunction<T> {
    @Override
    public void invoke(T value, Context context) throws Exception {
        // 模拟将数据写入文件
        System.out.println("写入文件: " + value);
    }
}
