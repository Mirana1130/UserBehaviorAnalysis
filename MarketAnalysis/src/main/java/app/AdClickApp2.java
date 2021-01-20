package app;

import bean.AdClickEvent;
import bean.AdCountByProvince;
import bean.BlackListWarning;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Timestamp;

/**
 * @author Mirana
 */
public class AdClickApp2 {
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
        //3.TODO 添加过滤逻辑，单日某个人点击某个广告达到100次，加入黑名单
        KeyedStream<AdClickEvent, Tuple> keyedStream1 = javabean.keyBy("userId", "adId");
        SingleOutputStreamOperator<AdClickEvent> process = keyedStream1.process(new BlackListProcessFunc(100L));
        //4.TODO 按省份分组
        KeyedStream<AdClickEvent, String> keyedStream = process.keyBy(AdClickEvent::getProvince);
        //5.TODO 开timeWindow(1h,5s)
        WindowedStream<AdClickEvent, String, TimeWindow> windowStream = keyedStream.timeWindow(Time.hours(1), Time.seconds(5));
        //6.TODO 用窗口流聚合函数（AggFunc,WinAllFunc）
        SingleOutputStreamOperator<AdCountByProvince> result = windowStream.aggregate(new AdClickAggFunc(), new AdClickWindowFunc());

        result.print("result");
        process.getSideOutput(new OutputTag<BlackListWarning>("BlackList") {
        }).print("Side");
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
    public static class AdClickWindowFunc implements WindowFunction<Long, AdCountByProvince,String,TimeWindow> {
        @Override
        public void apply(String s, TimeWindow window, Iterable<Long> input, Collector<AdCountByProvince> out) throws Exception {
            Long next = input.iterator().next();
            long end = window.getEnd();
            Timestamp timestamp = new Timestamp(end);
            out.collect(new AdCountByProvince(s,timestamp.toString(),next));
        }
    }
    public static class BlackListProcessFunc extends KeyedProcessFunction<Tuple,AdClickEvent,AdClickEvent>{
        //定义拉黑属性
      private Long maxClick;

      public BlackListProcessFunc(Long maxClick) {
          this.maxClick = maxClick;
      }
        //定义状态
      private ValueState<Long> countState;
      private ValueState<Boolean> isSendState;

      @Override
      public void open(Configuration parameters) throws Exception {
          countState=getRuntimeContext().getState(new ValueStateDescriptor<Long>("count-state",Long.class));
          isSendState=getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("issend-state",Boolean.class));
      }
      @Override
      public void processElement(AdClickEvent value, Context ctx, Collector<AdClickEvent> out) throws Exception {
          //获取状态
          Long count=countState.value();
          Boolean isSend = isSendState.value();
          //判断为第一条数据
          if (count == null){
              countState.update(1L);
              //注册定时器,时间为第二天零点
              long ts = (value.getTimestamp() / (60 * 60 * 24) + 1) * (24 * 60 * 60 * 1000L) - 8 * 60 * 60 * 1000L;
                ctx.timerService().registerEventTimeTimer(ts);
            }else {
              countState.update(count+1);
                //如果大于100
                if (count+1>=maxClick ){
                    if( isSend ==null){
                        ctx.output(new OutputTag<BlackListWarning>("BlackList"){},new BlackListWarning(value.getUserId(),value.getAdId(),"拉入黑名单！！"));
                        //更新状态
                        isSendState.update(true);
                    }
                    return;
                }
            }
          //输出数据
          out.collect(value);
      }
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<AdClickEvent> out) throws Exception {
            countState.clear();
            isSendState.clear();
        }
  }
}
