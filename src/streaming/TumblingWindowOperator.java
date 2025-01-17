package streaming;

import java.util.function.Function;

public class TumblingWindowOperator<T> implements DataStream<T> {
    private final DataStream<T> input;

    public TumblingWindowOperator(DataStream<T> input) {
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
        return null; // reduce 操作的实现
    }

    @Override
    public void addSink(SinkFunction<T> sink) {
        // 将数据送入 Sink
    }
}

