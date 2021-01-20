package app;

import bean.OrderEvent;
import bean.ReceiptEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class PayReceiptWithConnectApp {

    public static void main(String[] args) throws Exception {

//1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

//2.读取文本数据创建流,转换为JavaBean并提取时间戳生成Watermark
        SingleOutputStreamOperator<OrderEvent> orderEventDS = env.readTextFile("input/OrderLog.csv")
                //SingleOutputStreamOperator<OrderEvent> orderEventDS = env.socketTextStream("hadoop102", 9999)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new OrderEvent(Long.parseLong(fields[0]),
                            fields[1],
                            fields[2],
                            Long.parseLong(fields[3]));
                })
                .filter(data -> "pay".equals(data.getEventType()))
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderEvent>() {
                    @Override
                    public long extractAscendingTimestamp(OrderEvent element) {
                        return element.getEventTime() * 1000L;
                    }
                });

        SingleOutputStreamOperator<ReceiptEvent> receiptEventDS = env.readTextFile("input/ReceiptLog.csv")
                .map(line -> {
                    String[] fields = line.split(",");
                    return new ReceiptEvent(fields[0], fields[1], Long.parseLong(fields[2]));
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<ReceiptEvent>() {
                    @Override
                    public long extractAscendingTimestamp(ReceiptEvent element) {
                        return element.getTimestamp() * 1000L;
                    }
                });

//3.按照事务ID分组之后进行连接
        ConnectedStreams<OrderEvent, ReceiptEvent> connectedStreams = orderEventDS.keyBy(OrderEvent::getTxId)
                .connect(receiptEventDS.keyBy(ReceiptEvent::getTxId));
//4.使用ProcessFunction处理2个流的数据
        SingleOutputStreamOperator<String> result = connectedStreams.process(new PayReceiptKeyedProcessFunc());
        //5.打印数据
        result.print("Payed And Receipt");
        result.getSideOutput(new OutputTag<String>("Payed No Receipt") {
        }).print("Payed No Receipt");
        result.getSideOutput(new OutputTag<String>("No Payed But Receipt") {
        }).print("No Payed But Receipt");

        //6.执行
        env.execute();

    }
    public static class PayReceiptKeyedProcessFunc extends KeyedCoProcessFunction<String, OrderEvent, ReceiptEvent, String> {
    //定义状态
        private ValueState<OrderEvent> orderEventState;
        private ValueState<ReceiptEvent> receiptEventState;
        private ValueState<Long> tsState;
        @Override
        public void open(Configuration parameters) throws Exception {
            orderEventState = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("pay-state", OrderEvent.class));
            receiptEventState = getRuntimeContext().getState(new ValueStateDescriptor<ReceiptEvent>("receipt-state", ReceiptEvent.class));
            tsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts-state", Long.class));
        }
    //处理支付数据
        @Override
        public void processElement1(OrderEvent value, Context ctx, Collector<String> out) throws Exception {

         //判断到账数据是否已到达
            ReceiptEvent receiptEvent = receiptEventState.value();

            if (receiptEvent != null) {
            //到账数据比支付数据先到,正常数据
                out.collect(value.getTxId() + " Payed And Receipt" + receiptEvent.getTimestamp() + " " + value.getEventTime());
            //删除定时器
                ctx.timerService().deleteEventTimeTimer(tsState.value());
            //清空状态
                receiptEventState.clear();
                tsState.clear();
            } else {
            //到账数据还没有到达
                orderEventState.update(value);//TODO 34729,pay,sd76f87d6,1558430844
             //注册定时器
                long ts = (value.getEventTime() + 5) * 1000L;
                ctx.timerService().registerEventTimeTimer(ts);
                tsState.update(ts);
            }
        }
        //处理到账数据
        @Override
        public void processElement2(ReceiptEvent value, Context ctx, Collector<String> out) throws Exception {
            //判断支付数据是否已到达
            OrderEvent orderEvent = orderEventState.value();
            if (orderEvent != null) {
                //支付数据比到账数据先到,正常输出
                out.collect(value.getTxId() + " payed and receipt" + orderEvent.getEventTime() + " " + value.getTimestamp());
                //删除定时器
                ctx.timerService().deleteEventTimeTimer(tsState.value());
                //清空状态
                orderEventState.clear();
                tsState.clear();
            } else {
                //支付数据还没有到达
                receiptEventState.update(value);
                //注册定时器
                long ts = (value.getTimestamp() + 3) * 1000L;
                ctx.timerService().registerEventTimeTimer(ts);
                tsState.update(ts);
            }
        }
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            //判断支付是否为空
            OrderEvent orderEvent = orderEventState.value();
            ReceiptEvent receiptEvent = receiptEventState.value();
            if (orderEvent != null) {
                //说明只有支付数据,没有到账数据
                ctx.output(new OutputTag<String>("Payed No Receipt") {
                           },
                        orderEvent.getTxId() + " Payed No Receipt");
            } else {
                //说明只有到账数据,没有支付数据
                ctx.output(new OutputTag<String>("No Payed But Receipt") {
                           },
                        receiptEvent.getTxId() + " No Payed But Receipt");
            }
            //清空状态
            orderEventState.clear();
            receiptEventState.clear();

        }
    }

}