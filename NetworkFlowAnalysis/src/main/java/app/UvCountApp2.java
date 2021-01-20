package app;

import beans.UserBehavior;
import beans.UvCount;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.sql.Timestamp;
import java.util.HashSet;
import java.util.Iterator;

/**
 * @author Mirana
 */
public class UvCountApp2 {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
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
       //TODO 自定义触发器->不让系统默认的缓存数据，来一条数据就进行计算
        SingleOutputStreamOperator<UvCount> result = allwindowStream.trigger(new Mytrigger())
       //TODO 去重   process方法 （要和redis连接）
                .process(new UvProcess());
        result.print("result");
        env.execute();
    }
    public static class Mytrigger extends Trigger<UserBehavior, TimeWindow>{
        @Override
        public TriggerResult onElement(UserBehavior element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.FIRE_AND_PURGE;
        }
        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }
        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }
        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        }
    }
    public static class UvProcess extends ProcessAllWindowFunction<UserBehavior, UvCount, TimeWindow> {
        //声明Redis连接
        private Jedis jedisClient;
        //定义每个小时的RedisKey
        private String hourUvRedisKey;
        //定义布隆过滤器
        private MyBloomFilter myBloomFilter;
        @Override
        public void open(Configuration parameters) throws Exception {
            jedisClient=new Jedis("hadoop102",6379);
            hourUvRedisKey="HourUv";
            myBloomFilter=new MyBloomFilter(1<<30);
        }
        @Override
        public void process(Context context, Iterable<UserBehavior> elements, Collector<UvCount> out) throws Exception {
            //定义位图RedisKey
            Timestamp windowend = new Timestamp(context.window().getEnd());
            String bitMapRedisKey="BitMap"+windowend;
            String field = windowend.toString();
            long offset = myBloomFilter.getOffset(elements.iterator().next().getUserId().toString());
            Boolean getbit = jedisClient.getbit(bitMapRedisKey, offset);
            if(!getbit){
                jedisClient.setbit(bitMapRedisKey,offset,true);
                jedisClient.hincrBy(hourUvRedisKey,field,1L);
            }
            out.collect(new UvCount(field,Long.parseLong(jedisClient.hget(hourUvRedisKey,field))));
        }
        @Override
        public void close() throws Exception {
            jedisClient.close();
        }
    }
    public static class MyBloomFilter {
        private Long cap;
        public  MyBloomFilter(long cap){
            this.cap=cap;
        }
        public  long getOffset(String value){
            long result =0L;
            for (char c : value.toCharArray()) {
                result +=result*31 +c;
            }
            return result & (cap -1);
        }
    }
}