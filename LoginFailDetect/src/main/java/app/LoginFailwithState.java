package app;

import bean.LoginEvent;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;

//TODO 如果同一用户（可以是不同IP）在2秒之内连续两次登录失败，就认为存在恶意登录的风险
//按状态编程逻辑先做
public class LoginFailwithState {
    public static void main(String[] args) throws Exception {
        //1.执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2.从文件读取数据
        DataStream<LoginEvent> loginEventStream = env.readTextFile("input/LoginLog.csv")
                .map(line -> {
                    String[] fields = line.split(",");
                    return new LoginEvent(new Long(fields[0]), fields[1], fields[2], new Long(fields[3]));
                })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LoginEvent>(Time.seconds(5)) {
                    @Override
                    public long extractTimestamp(LoginEvent element) {
                        return element.getTimestamp() * 1000L;
                    }
                });
        //按照用户id分组
        KeyedStream<LoginEvent, Long> keyedStream = loginEventStream.keyBy(LoginEvent::getUserId);
        SingleOutputStreamOperator<String> result = keyedStream.process(new LoginFailProcessFunc(2));
        result.print();
        env.execute();

    }
    public static class LoginFailProcessFunc extends KeyedProcessFunction<Long,LoginEvent,String>{
        private int interval;
        public LoginFailProcessFunc(int interval) {
            this.interval = interval;
        }
        //TODO 1.定义状态
        private ListState<LoginEvent> failListState;//放失败数据
        private ValueState<Long> tsState;//放定时器时间

        //TODO 2.状态初始化
        @Override
        public void open(Configuration parameters) throws Exception {
            failListState=getRuntimeContext().getListState(new ListStateDescriptor<LoginEvent>("list",LoginEvent.class));
            tsState=getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts",Long.class));
        }
        @Override
        public void processElement(LoginEvent value, Context ctx, Collector<String> out) throws Exception {
            Iterable<LoginEvent> loginEvents = failListState.get();
            long ts = (value.getTimestamp() + interval) * 1000L;
            //-------------------------------------TODO--如果是失败数据---------------------------------------------
            if ("fail".equals(value.getEventType())){
                    // 数据放入状态
                    failListState.add(value);
                //-------------如果是第一条失败数据------------
                if (!loginEvents.iterator().hasNext()){
                    // 注册定时器
                    ctx.timerService().registerEventTimeTimer(ts);
                    tsState.update(ts);
                }
                //--------------------------------TODO----是成功数据-----------------------------------------------
            }else {
                if (tsState.value()!=null){
                   ctx.timerService().deleteEventTimeTimer(ts);
                }
                tsState.clear();
                failListState.clear();
            }
        }
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                //failListState状态里有两条及以上才要报警
            Iterator<LoginEvent> iterator = failListState.get().iterator();
            ArrayList<LoginEvent> loginEvents = Lists.newArrayList(iterator);
            if (loginEvents.size()>=2){
                out.collect(ctx.getCurrentKey()+" 在 "
                        +loginEvents.get(0).getTimestamp()
                        +" 到 "
                        +loginEvents.get(loginEvents.size()-1).getTimestamp()
                        + "时间里连续登陆"
                        +loginEvents.size()
                        +"次");
            }
            //清空状态
            failListState.clear();
            tsState.clear();
        }
    }
}
