package app;

import bean.AdClickEvent;
import bean.AdCountByProvince;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * @author Mirana
 */
public class AdClickApp {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境 TODO 规定时间语义为事件事件
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //2.读取文本文件 转化为javabean TODO 取时间字段 定watermark
        DataStreamSource<String> textDStream = env.readTextFile("input/AdClickLog.csv");
        SingleOutputStreamOperator<AdClickEvent> javabean = textDStream.map(new MapFunction<String, AdClickEvent>() {
            @Override
            public AdClickEvent map(String value) throws Exception {
                String[] fields = value.split(",");
                return new AdClickEvent(new Long(fields[0]), new Long(fields[1]), fields[2], fields[3], new Long(fields[4]));
            }
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<AdClickEvent>() {
            @Override
            public long extractAscendingTimestamp(AdClickEvent element) {
                return element.getTimestamp()*1000L;
            }
        });
        //3.TODO 按省份分组
        KeyedStream<AdClickEvent, String> keyedStream = javabean.keyBy(AdClickEvent::getProvince);
        //4.TODO 开timeWindow(1h,5s)
        WindowedStream<AdClickEvent, String, TimeWindow> windowStream = keyedStream.timeWindow(Time.hours(1),Time.seconds(5));
        //5.TODO 用窗口流聚合函数（AggFunc,WinAllFunc）
        SingleOutputStreamOperator<AdCountByProvince> result = windowStream.aggregate(new AdClickAggFunc(), new AdClickWindowFunc());
        result.print();
        env.execute();
    }
   public static  class AdClickAggFunc implements AggregateFunction<AdClickEvent,Long,Long>{
       @Override
       public Long createAccumulator() {
           return 0L;
       }

       @Override
       public Long add(AdClickEvent value, Long accumulator) {
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
   public static class AdClickWindowFunc implements WindowFunction<Long, AdCountByProvince,String,TimeWindow>{
      @Override
      public void apply(String s, TimeWindow window, Iterable<Long> input, Collector<AdCountByProvince> out) throws Exception {
          Long next = input.iterator().next();
          long end = window.getEnd();
          Timestamp timestamp = new Timestamp(end);
          out.collect(new AdCountByProvince(s,timestamp.toString(),next));
      }
  }
}
