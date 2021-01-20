package app;
import bean.OrderEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class OrderPayWithStateApp {
    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2.读取文本数据创建流,转换为JavaBean并提取时间戳生成Watermark
//        SingleOutputStreamOperator<OrderEvent> orderEventDS = env.readTextFile("input/OrderLog.csv")
        SingleOutputStreamOperator<OrderEvent> orderEventDS = env.socketTextStream("hadoop102", 9999)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new OrderEvent(Long.parseLong(fields[0]),
                            fields[1],
                            fields[2],
                            Long.parseLong(fields[3]));
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderEvent>() {
                    @Override
                    public long extractAscendingTimestamp(OrderEvent element) {
                        return element.getEventTime() * 1000L;
                    }
                });

        //3.按照OrderID进行分组
        KeyedStream<OrderEvent, Long> keyedStream = orderEventDS.keyBy(OrderEvent::getOrderId);

        //4.使用状态编程实现订单超时功能
        SingleOutputStreamOperator<String> result = keyedStream.process(new OrderTimeOutProcessFunc());

        //5.打印数据
        result.print("result");
        result.getSideOutput(new OutputTag<String>("payed timeout") {
        }).print("Payed TimeOut");
        result.getSideOutput(new OutputTag<String>("no pay") {
        }).print("No Pay");

        //6.执行
        env.execute();

    }

    public static class OrderTimeOutProcessFunc extends KeyedProcessFunction<Long, OrderEvent, String> {

        //定义状态用于保存创建数据
        private ValueState<OrderEvent> createState;

        private ValueState<Long> tsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            createState = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("create-state", OrderEvent.class));
            tsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts-state", Long.class));
        }

        @Override
        public void processElement(OrderEvent value, Context ctx, Collector<String> out) throws Exception {

            //判断当前数据类型
            if ("create".equals(value.getEventType())) {

                //更新状态并注册定时器
                createState.update(value);

                long ts = (value.getEventTime() + 900) * 1000L;
                ctx.timerService().registerEventTimeTimer(ts);

                tsState.update(ts);
            } else if ("pay".equals(value.getEventType())) {

                //取出状态数据
                OrderEvent orderEvent = createState.value();

                if (orderEvent == null) {
                    //说明支付数据到达的时候已经超过15分钟
                    ctx.output(new OutputTag<String>("payed timeout") {
                    }, value.getOrderId() + " Payed But TimeOut!");
                } else {
                    //说明15分钟以内支付的
                    out.collect(value.getOrderId() + " Create at " + orderEvent.getEventTime() +
                            ",Payed at " + value.getEventTime());
                    //删除定时器
                    ctx.timerService().deleteEventTimeTimer(tsState.value());
                    //清空状态
                    createState.clear();
                    tsState.clear();
                }
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

            //取出状态数据
            OrderEvent orderEvent = createState.value();

            System.out.println("aaa");

            //输出数据
            ctx.output(new OutputTag<String>("no pay") {
                       },
                    orderEvent.getOrderId() + " Pay TimeOut!");

            //清空状态
            createState.clear();
            tsState.clear();

        }
    }

}