package app;
import beans.ApacheLog;
import beans.UrlCount;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;

public class HotUrlApp {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //2.读取数据，转换为JavaBean,过滤，提取数据中的时间戳生成wm
//        DataStreamSource<String> apachelog = env.readTextFile("input/apache.log");
        //        //83.149.9.216 - - 17/05/2015:10:05:03 +0000 GET /presentations/logstash-monitorama-2013/images/kibana-search.png
        //        //83.149.9.216 - - 17/05/2015:10:05:43 +0000 GET /presentations/logstash-monitorama-2013/images/kibana-dashboard3.png
        //        //83.149.9.216 - - 17/05/2015:10:05:47 +0000 GET /presentations/logstash-monitorama-2013/plugin/highlight/highlight.js
        DataStreamSource<String> apachelog = env.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<ApacheLog> apacheLogDStream = apachelog.map(new MapFunction<String, ApacheLog>() {
            @Override
            public ApacheLog map(String value) throws Exception {
                SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
                String[] fields = value.split(" ");
                long time = sdf.parse(fields[3]).getTime();
                return new ApacheLog(fields[0], fields[1], time, fields[5], fields[6]);
            }
        })
                .filter(data -> "GET".equals(data.getMethod()))
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ApacheLog>(Time.seconds(2)) {
                    @Override
                    public long extractTimestamp(ApacheLog element) {
                        return element.getEventTime();
                    }
                });
        //3.按照url分组
        KeyedStream<ApacheLog, String> keyedSTream = apacheLogDStream.keyBy(ApacheLog::getUrl);
        //4.开窗（10min,5s,允迟1min）
        WindowedStream<ApacheLog, String, TimeWindow> windowDStream = keyedSTream.timeWindow(Time.minutes(10), Time.seconds(5))
                .allowedLateness(Time.minutes(1));
        //5.计算每个窗口内部的每个Url的访问次数，滚动聚合
        SingleOutputStreamOperator<UrlCount> UrlCountDStream = windowDStream.aggregate(new UrlCountAggFunc(), new UrlCountWindowFunc());
        //6.以窗口的结束时间分组
        KeyedStream<UrlCount, Long> keyedStream = UrlCountDStream.keyBy(UrlCount::getWindowEnd);
        //7.因为要将用到状态和定时器所以用process
        SingleOutputStreamOperator<String> result = keyedStream.process(new UrlCountKeyedProcessFunc(5));
       apachelog.print("apachelog");
       UrlCountDStream.print("agg");
        result.print("result");
        env.execute();
    }
        public static class UrlCountAggFunc implements AggregateFunction<ApacheLog,Long,Long> {
            @Override
            public Long createAccumulator() {
                return 0L;
            }
            @Override
            public Long add(ApacheLog value, Long accumulator) {
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
        public static class UrlCountWindowFunc implements WindowFunction<Long, UrlCount,String,TimeWindow>{
            @Override
            public void apply(String s, TimeWindow window, Iterable<Long> input, Collector<UrlCount> out) throws Exception {
                out.collect(new UrlCount(s,window.getEnd(),input.iterator().next()));
            }
        }
        public static class UrlCountKeyedProcessFunc extends KeyedProcessFunction<Long,UrlCount,String>{
            private int topsize;
            public UrlCountKeyedProcessFunc() {
            }
            public UrlCountKeyedProcessFunc(int topsize) {
                this.topsize = topsize;
            }
            //TODO 定义MapState属性
            private MapState<String,UrlCount> mapState;
            @Override
            public void open(Configuration parameters) throws Exception {
                mapState=getRuntimeContext().getMapState(new MapStateDescriptor<String, UrlCount>("mapState",String.class,UrlCount.class));
            }
            @Override
            public void processElement(UrlCount value, Context ctx, Collector<String> out) throws Exception {
                //来一条数据就加入状态
                mapState.put(value.getUrl(),value);
                //注册定时器 1000L保证之前的数据全部到齐
                ctx.timerService().registerEventTimeTimer(value.getWindowEnd()+1000L);
                ctx.timerService().registerEventTimeTimer(value.getWindowEnd()+60000L);
            }
            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                if (timestamp==ctx.getCurrentKey()+60000L){
                    mapState.clear();
                    return;
                }
                //1.因为已经对windowEnd分组了，所以直接取出状态里数据
                Iterator<Map.Entry<String, UrlCount>> iterator = mapState.entries().iterator();
                ArrayList<Map.Entry<String, UrlCount>> lists = Lists.newArrayList(iterator);
                //2.对wordcount做排序
                lists.sort(new Comparator<Map.Entry<String, UrlCount>>() {
                    @Override
                    public int compare(Map.Entry<String, UrlCount> o1, Map.Entry<String, UrlCount> o2) {
                        if (o1.getValue().getCount()>o2.getValue().getCount()){
                            return -1;
                        }else if (o1.getValue().getCount()<o2.getValue().getCount()){
                            return 1;
                        } else {
                            return 0;
                        }
                    }
                });
                //3.定义字符串 将结果数据封装
                StringBuilder stringBuilder = new StringBuilder();
                stringBuilder.append("------------------------");
                stringBuilder.append(new Timestamp(timestamp));
                stringBuilder.append("------------------------");
                stringBuilder.append("\n");
                for (int i = 0; i < Math.min(lists.size(),topsize); i++) {
                    //取出单条数据
                    UrlCount urlCount = lists.get(i).getValue();
                    stringBuilder.append("Top"+(i+1));
                    stringBuilder.append(" URL:").append(urlCount.getUrl());
                    stringBuilder.append(" Count:").append(urlCount.getCount());
                    stringBuilder.append("\n");
                }
                Thread.sleep(2000);
                out.collect(stringBuilder.toString());
            }
        }
}
