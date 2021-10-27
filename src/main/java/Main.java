import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.util.Random;


public class Main {
    public static void main(String[] args) throws Exception {



        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final DataSource<String> sourceData = env.readTextFile("/Users/libra/Documents/Gitee/NewTwoPhase/src/main/resources/dataset.txt").setParallelism(1);
        final FlatMapOperator<String, String> stringStringFlatMapOperator = sourceData.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                final String[] splits = s.split(":");
                final int length = splits[0].split(" ").length;
                final StringBuilder itemsNum = new StringBuilder();
                final StringBuilder itemsPrice = new StringBuilder();
                final Random random = new Random();
                for (int i = 0; i < length; i++) {
                    //随机生成项的个数，放在项的个数拼接
                    itemsNum.append(random.nextInt(100)+50);
                    itemsNum.append(" ");
                    itemsPrice.append(random.nextInt(450)+50);
                    itemsPrice.append(" ");
                }

                System.out.println(splits[0]+":"+itemsNum.toString()+":"+itemsPrice.toString());

                collector.collect(splits[0]+":"+itemsNum.toString()+":"+itemsPrice.toString());
//                collector.collect(splits[0]+":"+length);

            }
        }).setParallelism(1);


        stringStringFlatMapOperator
                .writeAsText("/Users/libra/Documents/Gitee/NewTwoPhase/src/main/resources/a.txt", FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);

        env.execute();



    }
}
