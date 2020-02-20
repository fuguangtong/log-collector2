package com.foo.udtf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;
import org.json.JSONException;

import java.util.ArrayList;
import java.util.List;

public class EventJsonUDTF extends GenericUDTF {

    //指定输出参数的名称和参数类型
    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs)
            throws UDFArgumentException {

        List<String> fileNames = new ArrayList<>();
        List<ObjectInspector> fileTypes = new ArrayList<>();

        fileNames.add("event_name");
        fileTypes.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        fileNames.add("event_json");
        fileTypes.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        //工厂方法
        return ObjectInspectorFactory.getStandardStructObjectInspector(fileNames,fileTypes);
    }
    //输入一条记录，输出若干条结果(et)
    @Override
    public void process(Object[] objects) throws HiveException {
        //获取传输的et
        String input = objects[0].toString();

        //判断传入的数据是否为空，如果为空则直接过滤掉该数据
        if(StringUtils.isBlank(input)){
            return;
        }else {
            //获取一共有几个事件
            try {
                JSONArray ja = new JSONArray(input);
                //判断里面是否有事件
                if(ja == null){
                    return;
                }
                //事件不为空的情况，遍历每一个事件
                for (int i=0; i < ja.length(); i++){
                    //定义一个字符串数组长度为2
                    String[] result = new String[2];
                    //取出每个事件的名称
                    result[0] = ja.getJSONObject(i).getString("en");
                    //取出每一个事件的整体
                    result[1] = ja.getString(i);
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
