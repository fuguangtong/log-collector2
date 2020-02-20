package com.foo.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;
import java.util.Map;

//类型拦截器
public class LogTypeInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        //获取flume的Body(方法体)，body为json数组
        byte[] body = event.getBody();
        //将json数据转换为字符串
        String jsonStr = new String(body);
        //声明一个字符串
        String logType="";
        //判断日志的类型，start为启动日志，其余的11种为其他日志类型
        if(jsonStr.contains("start")){
            logType="start";
        }else {
            logType="event";
        }
        //获取flume的消息头
        Map<String, String> headers = event.getHeaders();
        //把日志类型存储到flume的消息头当中
        headers.put("logType",logType);
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        for (Event event : list) {
            //调用拦截器
            intercept(event);
        }
        return list;
    }

    @Override
    public void close() {

    }
    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new LogTypeInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
