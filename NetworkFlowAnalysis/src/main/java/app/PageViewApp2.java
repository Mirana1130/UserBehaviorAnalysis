package app;

import beans.PvCount;
import beans.UserBehavior;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;

/**
 * @author Mirana
 */
public class PageViewApp2 {
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

        //3.将数据转换为KV结构 TODO 在key后+随机数
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = javabean.map(new MapFunction<UserBehavior, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(UserBehavior value) throws Exception {
                return new Tuple2<>("pv"+new Random().nextInt(8), 1);
            }
        }).keyBy(0);
        //4.开窗处理
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> wimdowDStream = keyedStream.timeWindow(Time.hours(1));

        //5.计算WordCount TODO 前面将数据散列开来 后面要用窗口时间聚合 要提取窗口时间
        SingleOutputStreamOperator<PvCount> pvcountDStream = wimdowDStream.aggregate(new PvAggFunc(), new PvWindowFunc());

        KeyedStream<PvCount, Long> keyedStream1 = pvcountDStream.keyBy(PvCount::getWindowEnd);
        SingleOutputStreamOperator<String> result = keyedStream1.process(new pvProcessFunc());


        //6.打印
        result.print();
        //7.执行
        env.execute();
    }
    public static class PvAggFunc implements AggregateFunction<Tuple2<String, Integer>,Long,Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Tuple2<String, Integer> value, Long accumulator) {
            return accumulator+1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a+b;
        }
    }
    public static class PvWindowFunc implements WindowFunction<Long, PvCount,Tuple,TimeWindow> {
        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Long> input, Collector<PvCount> out) throws Exception {
            long end = window.getEnd();
            out.collect(new PvCount(end,input.iterator().next()));
        }
    }
    public static class pvProcessFunc extends KeyedProcessFunction<Long,PvCount,String>{
        //声明状态
        private ListState<PvCount> listState;
        @Override
        public void open(Configuration parameters) throws Exception {
            listState=getRuntimeContext().getListState(new ListStateDescriptor<PvCount>("ListState",PvCount.class));
        }
        @Override
        public void processElement(PvCount value, Context ctx, Collector<String> out) throws Exception {
            //将进入的数据加入状态
            listState.add(value);
            //注册定时器 TODO 1000L为什么
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd()+1000L);
        }
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            //取出状态中的数据
            Iterator<PvCount> iterator = listState.get().iterator();
            //遍历累加
            Long count=0L;
            while (iterator.hasNext()){
                count+= iterator.next().getCount();
            }
            //清空状态
            listState.clear();
            //输出数据
            Timestamp timestamp1 = new Timestamp(timestamp - 1000L);
            out.collect("窗口结束时间：" + timestamp1 + "PvCount:" + count);
        }
    }
}
