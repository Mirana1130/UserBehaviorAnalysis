package app;

import bean.LoginEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
//TODO 如果同一用户（可以是不同IP）在2秒之内连续两次登录失败，就认为存在恶意登录的风险，
public class LoginFailWithState2 {
    public static void main(String[] args) throws Exception {
//1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//2.读取文本数据转换为JavaBean并提取时间戳生成Watermark
        SingleOutputStreamOperator<LoginEvent> loginEventDS = env.readTextFile("input/LoginLog.csv")
                .map(new MapFunction<String, LoginEvent>() {
                    @Override
                    public LoginEvent map(String value) throws Exception {
                        String[] fields = value.split(",");
                        return new LoginEvent(Long.parseLong(fields[0]),
                                fields[1],
                                fields[2],
                                Long.parseLong(fields[3]));
                    }
                })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LoginEvent>(Time.seconds(5)) {
                    @Override
                    public long extractTimestamp(LoginEvent element) {
                        return element.getTimestamp() * 1000L;
                    }
                });
//3.按照用户id分组
        KeyedStream<LoginEvent, Long> keyedStream = loginEventDS.keyBy(LoginEvent::getUserId);
//4.使用ProcessFunction处理数据
        SingleOutputStreamOperator<String> result = keyedStream.process(new LoginFailProcessFunc(2));
//5.打印数据
        result.print();
        env.execute();
    }
//4.TODO 自定义ProcessFunc extends KeyedProcessFunction<K, I, O>
    public static class LoginFailProcessFunc extends KeyedProcessFunction<Long, LoginEvent, String> {
    //1.定义属性
        private int interval;
        public LoginFailProcessFunc(int interval) {
            this.interval = interval;
        }
    //2.定义状态
        private ValueState<LoginEvent> failEventState;
    //3.open方法 给状态赋值
        @Override
        public void open(Configuration parameters) throws Exception {
            failEventState = getRuntimeContext().getState(new ValueStateDescriptor<LoginEvent>("fail-state", LoginEvent.class));
        }
    //4.processElement方法
        @Override
        public void processElement(LoginEvent value, Context ctx, Collector<String> out) throws Exception {
         //1.判断当前数据为失败数据
            if ("fail".equals(value.getEventType())) {
                //1.1取出状态数据
                LoginEvent lastFail = failEventState.value();
                failEventState.update(value);

                //1.2非第一条数据 比较时间间隔
                if (lastFail != null &&
                        Math.abs(value.getTimestamp() - lastFail.getTimestamp()) <= interval) {

                    //输出报警信息
                    out.collect(value.getUserId() + "连续登录失败两次！！！");
                }
            } else {
        //2.为成功数据 清空状态
                failEventState.clear();
            }
        }
    }
}