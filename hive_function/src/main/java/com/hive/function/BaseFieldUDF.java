package com.hive.function;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * @author annyu
 * @description hive udf 函数
 * @date 2020/5/29
 **/
public class BaseFieldUDF extends UDF {

    public  String evaluate(String line,String key)throws JSONException {
        String[] log = line.split("\\|");
        if(log.length!=2||StringUtils.isBlank(log[1])){
            return "";
        }
        JSONObject baseJson = new JSONObject(log[1].trim());

        String result="";
        //获取服务器时间
        if("st".equals(key)){
            return log[0].trim();
        }else if("et".equals(key)){
            //获取事件数
            if(baseJson.has("et")){
               result = baseJson.getString("et");
            }
        }else {
            JSONObject cm = baseJson.getJSONObject("cm");
            if(cm.has(key)){
                result=cm.getString(key);
            }
        }

        return result;

    }
}
