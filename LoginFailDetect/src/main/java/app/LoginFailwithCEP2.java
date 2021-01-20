package app;

import bean.LoginEvent;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;
//TODO CEP编程  循环模式 begin times
//TODO 如果同一用户（可以是不同IP）在2秒之内连续两次登录失败，就认为存在恶意登录的风险，
public class LoginFailwithCEP2 {
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
        //3.按照用户id分组
        KeyedStream<LoginEvent, Long> keyedStream = loginEventStream.keyBy(LoginEvent::getUserId);
        //4.定义模式序列连续两次失败 Pattern调 <泛型>begin方法（"起名（后面调用）"）
        Pattern<LoginEvent, LoginEvent> pattern =
                Pattern.<LoginEvent>begin("start")
                        .where(new SimpleCondition<LoginEvent>() {
                            @Override
                            public boolean filter(LoginEvent value) throws Exception {
                                return "fail".equals(value.getEventType());
                            }
                        })
                        //TODO 循环2次  默认是宽松模式
                        .times(2)
                        .consecutive() //设置严格模式
                        //TODO 规定时间内 调成5s 方便测试
                         .within(Time.seconds(5));
        //5.用CEP调用pattern方法 将Pattern用在流上
        PatternStream<LoginEvent> patternStream = CEP.pattern(keyedStream, pattern);
        //6.PatternStream用select（PatternSelectFunction）取出结果
        SingleOutputStreamOperator<String> reslut = patternStream.select(new MyPatternSelectFunc());
        //打印，环境执行
        reslut.print();
        env.execute();
    }
    //7.自定义PatternSelectFunction方法
    public static class MyPatternSelectFunc implements PatternSelectFunction<LoginEvent,String>{
        @Override
        public String select(Map<String, List<LoginEvent>> pattern) throws Exception {
            //TODO pattern.get取出LoginEvent的list,我们这里是循环模式只有key只有一个start  list值多个
            List<LoginEvent> all = pattern.get("start");
            LoginEvent start = all.get(0);
            LoginEvent last = all.get(all.size()-1);
            //TODO 返回结果
            return start.getUserId() + "在 " + start.getTimestamp() +"到 " + last.getTimestamp()+ " 之间连续登陆失败"+all.size()+"次";
        }
    }
}
