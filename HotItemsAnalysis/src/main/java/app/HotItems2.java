package app;

import beans.ItemCount;
import beans.UserBehavior;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;

public class HotItems2 {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(env, bsSettings);
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
        Table userBehaviortable = bsTableEnv.fromDataStream(userBehaviorDS,"itemId,behavior,timestamp.rowtime as ts");
        bsTableEnv.createTemporaryView("userBehavior",userBehaviortable);
        String sql="select itemId,count(itemId) as cnt,hop_end(ts,interval '5' minute,interval '1' hour) as windowEnd from userBehavior where behavior='pv' group by itemId, hop(ts,interval '5' minute,interval '1' hour)";
        Table counttable = bsTableEnv.sqlQuery(sql);

        bsTableEnv.createTemporaryView("counttable",counttable);
        String sqltemp="select *,row_number() over(partition by windowEnd order by cnt desc) as rk from counttable";
        Table temptable = bsTableEnv.sqlQuery(sqltemp);

        bsTableEnv.createTemporaryView("temp",temptable);
        String sqltop="select * from temp where rk <=5";
        Table toptable = bsTableEnv.sqlQuery(sqltop);

        DataStream<Tuple2<Boolean, Row>> result = bsTableEnv.toRetractStream(toptable, Row.class);
        result.print("result");
        env.execute();
    }
}
