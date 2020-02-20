package com.foo.udf;

import groovy.json.JsonOutput;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONException;
import org.json.JSONObject;

public class BaseFieldUDF extends UDF {
    //重写evaliuate方法
    public String evaluate(String line,String jsonKeysString){
           StringBuilder sb = new StringBuilder();
           //切割json key
           String[] jsonKeys = jsonKeysString.split(",");
           //处理line   服务器时间|json
           String[] logContents = line.split("\\|");
           //合法性校验长度和为空
           if(logContents.length!=2 || StringUtils.isBlank(logContents[1])){
               return "";
           }
           //处理json
           try {
               JSONObject jsonObject = new JSONObject(logContents[1]);
               //获取cm里面的对象
               JSONObject base = jsonObject.getJSONObject("cm");
               for(int i=0;i<jsonKeys.length;i++){
                   String filedName = jsonKeys[i].trim();
                   if(base.has(filedName)){
                        sb.append(base.getString(filedName)).append("\t");
                   }else {
                       sb.append("").append("\t");
                   }
               }
               sb.append(jsonObject.getString("et")).append("\t");
               sb.append(logContents[0]).append("\t");
           } catch (JSONException e) {
               e.printStackTrace();
       }

       return sb.toString();
   }

    public static void main(String[] args) {
            String line  = "1577437388936|{\"cm\":{\"ln\":\"-51.6\",\"sv\":\"V2.4.9\",\"os\":\"8.2.2\",\"g\":\"4K6BRGL4@gmail.com\",\"mid\":\"m037\",\"nw\":\"3G\",\"l\":\"pt\",\"vc\":\"5\",\"hw\":\"640*1136\",\"ar\":\"MX\",\"uid\":\"u008\",\"t\":\"1577375231091\",\"la\":\"20.8\",\"md\":\"HTC-17\",\"vn\":\"1.1.2\",\"ba\":\"HTC\",\"sr\":\"W\"},\"ap\":\"gmall\",\"et\":[{\"ett\":\"1577416382120\",\"en\":\"loading\",\"kv\":{\"extend2\":\"\",\"loading_time\":\"4\",\"action\":\"3\",\"extend1\":\"\",\"type\":\"1\",\"type1\":\"433\",\"loading_way\":\"1\"}},{\"ett\":\"1577366126570\",\"en\":\"ad\",\"kv\":{\"entry\":\"1\",\"show_style\":\"1\",\"action\":\"4\",\"detail\":\"325\",\"source\":\"3\",\"behavior\":\"1\",\"content\":\"2\",\"newstype\":\"6\"}},{\"ett\":\"1577386920949\",\"en\":\"active_background\",\"kv\":{\"active_source\":\"3\"}},{\"ett\":\"1577376802342\",\"en\":\"error\",\"kv\":{\"errorDetail\":\"java.lang.NullPointerException\\\\n    at cn.lift.appIn.web.AbstractBaseController.validInbound(AbstractBaseController.java:72)\\\\n at cn.lift.dfdf.web.AbstractBaseController.validInbound\",\"errorBrief\":\"at cn.lift.appIn.control.CommandUtil.getInfo(CommandUtil.java:67)\"}},{\"ett\":\"1577425314731\",\"en\":\"praise\",\"kv\":{\"target_id\":4,\"id\":8,\"type\":1,\"add_time\":\"1577376182820\",\"userid\":7}}]}";
            String jsonKeysString ="uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,nw,ln,la,t";
        String evaluate = new BaseFieldUDF().evaluate(line, jsonKeysString);
        System.out.println(evaluate);
    }
}
