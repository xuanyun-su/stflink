package com.atguigu.chapter05;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.ArrayList;
import java.util.Properties;

public class SourceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//
//        DataStreamSource<String> stream1 = env.readTextFile("input/clicks.txt");
//        // 从文件中读取数据
//        ArrayList<Integer> nums = new ArrayList<>();
//        nums.add(2);
//        nums.add(5);
//        DataStreamSource<Integer> numStream = env.fromCollection(nums);
//        ArrayList<Event> events = new ArrayList<>();
//        // 2. 从集合中读取数据
//        events.add(new Event("Mary","./home",1000L));
//        events.add(new Event("Bob","./cart",2000L));
//        DataStreamSource<Event> stream2 = env.fromCollection(events);
//        // 3. 从元素读取数据
//        DataStreamSource<Event> stream3 = env.fromElements(
//                new Event("Mary", "./home", 1000L),
//                new Event("Bob", "./cart", 2000L)
//        );
        // 4 从socket文本六渡桥
//        DataStreamSource<String> stream4 = env.socketTextStream("hadoop102", 7777);

//        stream3.print("3");
//        stream1.print("1");
//        stream2.print("2");
//        numStream.print("num");
//        stream4.print();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","hadoop102:9092");


        DataStreamSource<String> kafkaStream = env.addSource(new FlinkKafkaConsumer<String>("clicks", new SimpleStringSchema(), properties));
        kafkaStream.print();


        env.execute();
    }
}

