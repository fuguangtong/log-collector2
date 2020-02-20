package com.foo.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class LogETLInterceptor implements Interceptor {

    //初始化方法
    @Override
    public void initialize() {

    }

    //event一个事件
    @Override
    public Event intercept(Event event) {
        //获取传输过来的数据，且转换为字符串(此处body为原始数据，需要处理)
        String body = new String(event.getBody(), Charset.forName("UTF-8"));
        //判断原始数据是否符合
        if(LogUtils.validateReportLog(body)){
            //通过了校验是目标数据
           return event;
        }
        //没有通过校验返回null
        return null;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        //创建一个集合，长度为event集合的长度
       List<Event> listEvent = new ArrayList<Event>(list.size());
        for (Event event : list) {
            //调用验证log日志是否符合的方法
            Event interceptEvent = intercept(event);
            if(interceptEvent != null){
                //把不为空的加进去
                listEvent.add(interceptEvent);
            }
        }
        return listEvent;
    }

    @Override
    public void close() {

    }
    //创建内部类
    public static class Builder implements Interceptor.Builder{

        //调用LogETLInterceptor类
        @Override
        public Interceptor build() {
            return new LogETLInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
