package app;

import beans.PvCount;
import beans.UserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @author Mirana
 */
public class PageViewApp {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
         env.setParallelism(8);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2.读取文本数据创建流,转换为JavaBean,同时提取数据中的时间戳生成Watermark
        DataStreamSource<String> textDStream = env.readTextFile("input/UserBehavior.csv");

        SingleOutputStreamOperator<UserBehavior> javabean = textDStream.map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String value) throws Exception {
                String[] fields = value.split(",");
                return new UserBehavior(Long.parseLong(fields[0]), Long.parseLong(fields[1]), Integer.parseInt(fields[2]), fields[3], Long.parseLong(fields[4]));
            }
        }).filter(data -> "pv".equals(data.getBehavior()))
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(UserBehavior element) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        //3.将数据转换为KV结构
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = javabean.map(new MapFunction<UserBehavior, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(UserBehavior value) throws Exception {
                return new Tuple2<>("pv", 1);
            }
        }).keyBy(0);
        //4.开窗处理
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> wimdowDStream = keyedStream.timeWindow(Time.hours(1));

        //5.计算WordCount

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = wimdowDStream.sum(1);
        //6.打印
        result.print();
        //7.执行
        env.execute();
    }
}
