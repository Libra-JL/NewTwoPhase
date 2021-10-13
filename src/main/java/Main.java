import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

public class Main {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final DataSource<String> sourceData = env.readTextFile("/Users/libra/Documents/Gitee/NewTwoPhase/src/main/resources/dataset.txt").setParallelism(1);
        final FlatMapOperator<String, String> stringStringFlatMapOperator = sourceData.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                final String[] split = s.split(":");
                collector.collect(split[0].toString());
            }
        }).setParallelism(1);


        stringStringFlatMapOperator
                .writeAsText("/Users/libra/Documents/Gitee/NewTwoPhase/src/main/resources/a.txt", FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);

        env.execute();



    }
}
