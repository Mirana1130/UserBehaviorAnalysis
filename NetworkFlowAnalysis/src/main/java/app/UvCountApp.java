package app;

import beans.UserBehavior;
import beans.UvCount;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.HashSet;
import java.util.Iterator;

/**
 * @author Mirana
 */
public class UvCountApp {
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
        //3.开窗
        AllWindowedStream<UserBehavior, TimeWindow> allwindowStream = javabean.timeWindowAll(Time.hours(1));
        SingleOutputStreamOperator<UvCount> UvcountStream = allwindowStream.apply(new UvCountAllWindowFunc());
        UvcountStream.print();
        env.execute();
    }

    public static class UvCountAllWindowFunc implements AllWindowFunction<UserBehavior, UvCount, TimeWindow> {
        @Override
        public void apply(TimeWindow window, Iterable<UserBehavior> values, Collector<UvCount> out) throws Exception {
            //创建hashset
            HashSet<Long> uids = new HashSet<>();
            //
            Iterator<UserBehavior> iterator = values.iterator();
            while (iterator.hasNext()){
                uids.add(iterator.next().getUserId());
            }
            //
            long end = window.getEnd();
            Timestamp timestamp = new Timestamp(end);
            String string = timestamp.toString();
            out.collect(new UvCount(string,(long) uids.size()));
        }
    }
}