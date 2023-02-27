package org.example.fx;

import java.lang.reflect.Field;

public class SQLHelper {
    public static String getSqlInset(String tableName,Field[] fields ) {
        try {
            //获取实体的类名，需与表名一致
            //生成INSERT INTO table(field1,field2) 部分
            StringBuffer sbField = new StringBuffer();
            //生成VALUES('value1','value2') 部分
            StringBuffer sbValue = new StringBuffer();
            sbField.append("INSERT INTO " + tableName.toLowerCase() + "(");
            int fieldLength = fields.length;
            for(int i=0;i<fieldLength;i++){
                fields[i].setAccessible(true);
                sbField.append(fields[i].getName().toLowerCase()+',');
                sbValue.append("? ,");
            }
            return sbField.replace(sbField.length()-1, sbField.length(), ") VALUES(").append(sbValue.replace(sbValue.length()-1, sbValue.length(), ")")).toString();
        } catch (Exception e1) {
            e1.printStackTrace();
        }
        return null;
    }
}
