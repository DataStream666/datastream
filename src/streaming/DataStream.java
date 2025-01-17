package streaming;

import java.util.function.Function;

public interface DataStream<T> {
    DataStream<T> map(Function<T, T> mapper);
    DataStream<T> keyBy(Function<T, String> keySelector);
    DataStream<T> reduce(Function<T, T> reducer);
    void addSink(SinkFunction<T> sink);
}
