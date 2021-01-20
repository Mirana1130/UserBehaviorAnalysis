package app;

import beans.ItemCount;
import beans.UserBehavior;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.stream.Stream;

public class HotItems {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //2.读取文本数据创建流，同时提取时间戳，生成watermark
        SingleOutputStreamOperator<UserBehavior> userBehaviorDS = env.readTextFile("input/UserBehavior.csv").map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String value) throws Exception {
                String[] fields = value.split(",");
                return new UserBehavior(Long.parseLong(fields[0]), Long.parseLong(fields[1]), Integer.parseInt(fields[2]), fields[3], Long.parseLong(fields[4]));
            }
        })
                .filter(data -> "pv".equals(data.getBehavior()))
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(UserBehavior element) {
                        return element.getTimestamp() * 1000L;
                    }
                });
        //3.按照商品id分组
        KeyedStream<UserBehavior, Long> keyedStream = userBehaviorDS.keyBy(UserBehavior::getItemId);
        //4.开窗，滑动窗口（1h，5min）
        WindowedStream<UserBehavior, Long, TimeWindow> windowedStream = keyedStream.timeWindow(Time.hours(1), Time.minutes(5));
        //5.TODO 聚合计算，计算窗口内部每个商品被点击次数（滚动聚合来一条计算一条，窗口函数保证可以拿到窗口时间）
        SingleOutputStreamOperator<ItemCount> ItemCountDStream = windowedStream.aggregate(new ItemCountAggFunc(), new ItemCountWindowFunc());
        //6.TODO 按照窗口时间分组
        KeyedStream<ItemCount, Long> keyedStream2 = ItemCountDStream.keyBy(ItemCount::getWindowEnd);
        //7.TODO 处理Top5
        SingleOutputStreamOperator<String> result = keyedStream2.process(new ItemKeyedProcessFunc(5));
        result.print();
        env.execute();
    }
    //AggregateFunction（滚动聚合来一条计算一条）
    public static class ItemCountAggFunc implements AggregateFunction<UserBehavior,Long,Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }
        @Override
        public Long add(UserBehavior value, Long accumulator) {
            //TODO 为什么是+1  不是+=
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
    //(AggregateFunction->WindowFunction窗口函数保证可以拿到窗口时间)
    public static class ItemCountWindowFunc implements WindowFunction<Long,ItemCount,Long,TimeWindow>{
        @Override
        public void apply(Long itemID, TimeWindow window, Iterable<Long> input, Collector<ItemCount> out) throws Exception {
            //获取key --> itemId,获取窗口的结束时间,获取当前窗口中当前ItemID的点击次数
            out.collect(new ItemCount(itemID,window.getEnd(),input.iterator().next()));
        }
    }

    //使用ProcessFunction实现收集每个窗口中的数据做排序输出(状态编程--ListState  定时器)
    public static class ItemKeyedProcessFunc extends KeyedProcessFunction<Long, ItemCount,String>{
        private int topsize;
        public ItemKeyedProcessFunc(){
        }
        public ItemKeyedProcessFunc(int topsize){
            this.topsize=topsize;
        }
        //声明状态
        private ListState<ItemCount> listState;
        @Override
        public void open(Configuration parameters) throws Exception {
            listState =getRuntimeContext().getListState(new ListStateDescriptor<ItemCount>("listState",ItemCount.class));
        }
        @Override
        public void processElement(ItemCount value, Context ctx, Collector<String> out) throws Exception {
            //来一条数据就加入状态
            listState.add(value);
            //注册定时器 1000L保证之前的数据全部到齐
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd()+1000L);
        }
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            Iterator<ItemCount> iterator = listState.get().iterator();
            ArrayList<ItemCount> itemCounts = Lists.newArrayList(iterator);
            //对wordcount做排序
            itemCounts.sort(new Comparator<ItemCount>() {
                @Override
                public int compare(ItemCount o1, ItemCount o2) {
                    if (o1.getCount()>o2.getCount()){
                    return -1;
                    }else if(o1.getCount()<o2.getCount()){
                        return 1;
                    }else { return 0;}
                }
            });
            //定义字符串 封装结果
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("----------------");
            stringBuilder.append(new Timestamp(timestamp-1000L));
            stringBuilder.append("----------------");
            stringBuilder.append("\n");

            for (int i = 0; i < Math.min(topsize,itemCounts.size()); i++) {
                ItemCount itemCount = itemCounts.get(i);
                stringBuilder.append("Top"+(i+1));
                stringBuilder.append(" ItemId:").append(itemCount.getItemId());
                stringBuilder.append(" Count:").append(itemCount.getCount());
                stringBuilder.append("\n");
            }
            Thread.sleep(2000);
            listState.clear();
            out.collect(stringBuilder.toString());

        }
    }
}
