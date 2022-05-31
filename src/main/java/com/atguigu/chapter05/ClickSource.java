package com.atguigu.chapter05;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

public class ClickSource implements SourceFunction<Event> {
    // 声明一个标志类
    private Boolean running = true;

    @Override


    public void run(SourceContext<Event> ctx) throws Exception {

        // 随机生成数据
        Random random = new Random();
        // 循环生成数据
        String[] users = {"mary","Alice","Bob","Cary"};
        String[] urls = {"./homes","./cart","/fav","/prod?id=10"};



        while(running){
            String user = users[random.nextInt(users.length)];
            String url = urls[random.nextInt(urls.length)];
            Long timeStamp = Calendar.getInstance().getTimeInMillis();
            ctx.collect(new Event(user,url,timeStamp));
            Thread.sleep(1000L);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
