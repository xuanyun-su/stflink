package com.atguigu.chapter05;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformSimpleAggTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Bob", "./cart?id=1", 2500L),
                new Event("Bob", "./cart?id=10", 3000L));



        // 按键分组进行聚合,提取当前用户最近一次访问
        stream.keyBy(new KeySelector<Event, String>() {
            @Override
            public String getKey(Event value) throws Exception {
                return value.user; // 返回用户
            }
            // 选取最大时间戳 pojo类型用max的传字段对于传maxBy对于序列靠
            // max只针对 字段
        }).max("timestamp")
                .print("max:");

        stream.keyBy(data -> data.user)
                // 直接把完整数据拿出来
                .maxBy("timestamp")
                .print("maxyBy:");

        env.execute();

    }
}
