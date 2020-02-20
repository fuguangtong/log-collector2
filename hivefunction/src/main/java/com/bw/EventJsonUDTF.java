package com.bw;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;
import org.json.JSONException;

import java.util.ArrayList;
import java.util.List;

public class EventJsonUDTF extends GenericUDTF {

    //设置输出的名字和输出类型
    public StructObjectInspector initialize(ObjectInspector[] argOIs)
            throws UDFArgumentException {
        List<String> fileNames = new ArrayList<>();
        List<ObjectInspector> fileTypes = new ArrayList<>();

        //设置输出的名event_name
        fileNames.add("event_name");
        //设置输出类型
        fileTypes.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        fileNames.add("event_json");
        fileTypes.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fileNames,fileTypes);
    }

    //输入一个输出多个
    @Override
    public void process(Object[] objects) throws HiveException {
        //获取传输来的et
        String input = objects[0].toString();
        //判断是否为空,为空则直接过滤
        if (StringUtils.isBlank(input)){
            return;
        }else {
            //获取事件总个数
            try {
                JSONArray jsonArray = new JSONArray(input);
                //判断是否有事件
                if(jsonArray ==null){
                    return;
                }
                //遍历事件
                for (int i = 0; i < jsonArray.length(); i++) {
                    //声明一个字符串数组长度为二
                    String[] result = new String[2];
                    //获取en,简称每个事件的名称
                    result[0] = jsonArray.getJSONObject(i).getString("en");
                    //获取每一个事件的整体
                    result[1] = jsonArray.getString(i);
                    forward(result);
                }
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }

    }

    @Override
    public void close() throws HiveException {

    }
}
