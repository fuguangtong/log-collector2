package com.bw;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONException;
import org.json.JSONObject;

public class Demo1 extends UDF {
    //(一整条数据,json的key)
    public String evaluate(String line,String jsonKeysString){
        StringBuilder sb = new StringBuilder();
        //切割json key
        String[] jsonKeys = jsonKeysString.split(",");
        //切割整条数据|
        String[] lines = line.split("\\|");

        //校验长度和是否为空
        if(lines.length!=2 || StringUtils.isBlank(lines[1])){
                return "";
        }else {//处理json
            try {
                //服务器时间|json串
                JSONObject jsonObject = new JSONObject(lines[1]);
                //获取cm对象
                JSONObject base = jsonObject.getJSONObject("cm");
                for (int i = 0; i < jsonKeys.length; i++) {
                    //获取所有的key
                    String fileName = jsonKeys[i].trim();
                    //获取base里面是否包含与filename相同的key
                    if (base.has(fileName)){
                        //获取包含key的值并追加
                        sb.append(base.getString(fileName)).append("\t");
                    }else {
                        //如果不包含则给他追加一个空值
                        sb.append("").append("\t");
                    }
                }
                //获取et对象且追加
                sb.append(jsonObject.getString("et")).append("\t");
                //获取服务器时间戳且追加
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
        String evaluate = new Demo1().evaluate(line, jsonKeysString);
        System.out.println(evaluate);
    }
}
