import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Main {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final DataStreamSource<String> sourceData = env.readTextFile("/Users/libra/Documents/Gitee/NewTwoPhase/src/main/resources/dataset.txt");

        final SingleOutputStreamOperator<String> stringSingleOutputStreamOperator = sourceData.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                final String[] split = s.split(":");
                for (String s1 : split) {
                    collector.collect(s1);
                }
            }
        });

        stringSingleOutputStreamOperator.print();

        env.execute("test");
    }
}
