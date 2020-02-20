package com.bw;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONException;
import org.json.JSONObject;

public class BaseFieldUDF extends UDF {
    public String evaluate(String line,String jsonKeysString){
        StringBuilder sb = new StringBuilder();
        //切割jsonKeys
        String[] jsonKeys = jsonKeysString.split(",");
        //处理line
        String[] lines = line.split("\\|");
        //校验长度和lines的第二位是否为空
        if(lines.length!=2 || StringUtils.isBlank(lines[1])){
                return "";
        }else {
            //操作json
            try {
                JSONObject jsonObject = new JSONObject(lines[1]);
                //获取cm对象
                JSONObject base = jsonObject.getJSONObject("cm");
                //遍历json 的key
                for (int i = 0; i < jsonKeys.length; i++) {
                    //获取全部的字段
                    String columns = jsonKeys[i].trim();
                    //判断sm对象是否包含jsonKeys里面的key值
                   if(base.has(columns)){
                        sb.append(base.getString(columns)).append("\t");
                   }else {
                       sb.append("").append("\t");
                   }
                }
                //获取et对象
                sb.append(jsonObject.getString("et")).append("\t");
                //获取时间戳
                sb.append(lines[0]).append("\t");

            } catch (JSONException e) {
                e.printStackTrace();
            }

        }
        return sb.toString();
    }

    public static void main(String[] args) {
        String line= "1577437388993|{\"cm\":{\"ln\":\"-59.8\",\"sv\":\"V2.0.4\",\"os\":\"8.2.3\",\"g\":\"53DO92E3@gmail.com\",\"mid\":\"m276\",\"nw\":\"4G\",\"l\":\"es\",\"vc\":\"2\",\"hw\":\"640*960\",\"ar\":\"MX\",\"uid\":\"u985\",\"t\":\"1577396180867\",\"la\":\"-5.2\",\"md\":\"sumsung-17\",\"vn\":\"1.0.4\",\"ba\":\"Sumsung\",\"sr\":\"A\"},\"ap\":\"gmall\",\"et\":[{\"ett\":\"1577435762360\",\"en\":\"newsdetail\",\"kv\":{\"entry\":\"3\",\"newsid\":\"n794\",\"news_staytime\":\"5\",\"loading_time\":\"12\",\"action\":\"2\",\"showtype\":\"1\",\"category\":\"59\",\"type1\":\"\"}},{\"ett\":\"1577357026280\",\"en\":\"notification\",\"kv\":{\"ap_time\":\"1577366928234\",\"action\":\"1\",\"type\":\"2\",\"content\":\"\"}},{\"ett\":\"1577424360779\",\"en\":\"active_background\",\"kv\":{\"active_source\":\"2\"}},{\"ett\":\"1577432621193\",\"en\":\"comment\",\"kv\":{\"p_comment_id\":0,\"addtime\":\"1577379208642\",\"praise_count\":803,\"other_id\":2,\"comment_id\":4,\"reply_count\":56,\"userid\":6,\"content\":\"记降学才谰\"}},{\"ett\":\"1577408065481\",\"en\":\"favorites\",\"kv\":{\"course_id\":6,\"id\":0,\"add_time\":\"1577360002028\",\"userid\":8}},{\"ett\":\"1577355929823\",\"en\":\"praise\",\"kv\":{\"target_id\":0,\"id\":3,\"type\":1,\"add_time\":\"1577352772523\",\"userid\":5}}]}";
        String jsonKeysString  = "uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,nw,ln,la,t";
        String evaluate = new BaseFieldUDF().evaluate(line, jsonKeysString);
        System.out.println(evaluate);
    }
}
