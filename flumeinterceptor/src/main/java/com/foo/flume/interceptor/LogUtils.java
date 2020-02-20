package com.foo.flume.interceptor;


import org.apache.commons.lang.math.NumberUtils;
import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogUtils {
    //初始化logger报错日志
    private static Logger logger = LoggerFactory.getLogger(LogUtils.class);
    //验证是否否和要求的工具类
    public static boolean validateReportLog(String body) {
     //1577437388967|{"cm":{"ln":"-49.1","sv":"V2.3.0","os":"8.0.6","g":"68DIMQEK@gmail.com","mid":"m651","nw":"WIFI","l":"en","vc":"14","hw":"750*1134","ar":"MX",
        // "uid":"u138","t":"1577416141194","la":"-39.9","md":"Huawei-19","vn":"1.3.1","ba":"Huawei","sr":"W"},"ap":"gmall","et":[]}
        String[] split = body.split("\\|");
        /*
        /日志检查，正确的返回true，错误的返回false
         */
        try {
            //判断字符串以|切割后长度是否小于二，如果小于则不符合(校验的是总长度)
            if(split.length<2){
                return false;
            }
            //判断第一个是否为时间戳,且为数字的情况下
            if(split[0].length()!=13 || !NumberUtils.isDigits(split[0])){
                return false;
            }
            //判断第二个是否为正确的json串
            if(!split[1].trim().startsWith("{") || !split[1].trim().endsWith("}")){
                return false;
            }

        }catch (Exception e){
            //错误打印的日志(错误解析,正确的为+)
            logger.error("error parse,message is"+body);
            //打印抓到的错误日志
            logger.error(e.getMessage());
            return false;
        }
        //符合验证规则
        return true;
    }
}
