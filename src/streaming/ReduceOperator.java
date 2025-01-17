package streaming;

import java.util.function.Function;

public class ReduceOperator<T> implements DataStream<T> {
    private final DataStream<T> input;

    public ReduceOperator(DataStream<T> input) {
        this.input = input;
    }

    @Override
    public DataStream<T> map(Function<T, T> mapper) {
        return null; // map 操作的实现
    }

    @Override
    public DataStream<T> keyBy(Function<T, String> keySelector) {
        return null; // keyBy 操作的实现
    }

    @Override
    public DataStream<T> reduce(Function<T, T> reducer) {
        // 实现 reduce 操作
        return new ReduceOperator<>(input);
    }

    @Override
    public void addSink(SinkFunction<T> sink) {
        // 将数据送入 Sink
    }
}

