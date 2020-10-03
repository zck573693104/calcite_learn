package org.apache.calcite;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.Properties;


public class Client {
    /**
     * 测试的时候用字符串 defaultSchema 默认数据库 name 数据库名称 type custom factory
     * 请求接收类，该类会实例化Schema也就是数据库类，Schema会实例化Table实现类，Table会实例化数据类。
     * operand 动态参数，ScheamFactory的create方法会接收到这里的数据
     */
    public static void main(String[] args) throws Exception {
        String model = "/load/data/model.json";
        String sql = "select age from test where age > 20 ";
        dealCsv(model,sql);


    }

    static void dealCsv(String model,String sql) throws SQLException {
        Properties info = new Properties();
        info.put("model", model);
        Connection  connection = DriverManager.getConnection("jdbc:calcite:caseSensitive=false;unquotedCasing=UNCHANGED", info);
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);

        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnSize = metaData.getColumnCount();
        while (resultSet.next()) {
            Map<String, Object> map = Maps.newLinkedHashMap();
            for (int i = 1; i < columnSize + 1; i++) {
                map.put(metaData.getColumnName(i), resultSet.getObject(i));
            }
            System.out.println(JSONObject.toJSONString(map));
        }
    }





    public static void getData(ResultSet resultSet)throws Exception{
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnSize = metaData.getColumnCount();

        while (resultSet.next()) {
            Map<String, Object> map = Maps.newLinkedHashMap();
            for (int i = 1; i < columnSize + 1; i++) {
                map.put(metaData.getColumnLabel(i), resultSet.getObject(i));
            }
            System.out.println(JSONObject.toJSONString(map));
        }
    }
}