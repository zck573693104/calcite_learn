package org.apache.calcite;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import org.apache.calcite.util.Sources;
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
        String sql = "select * from test where age > 20 ";
        dealCsv(model,sql);

            // 字符串方式
            //String model = Sources.of(Client.class.getResource("/" + "model.json")).file().getAbsolutePath();

            Properties info = new Properties();
            info.put("model", model);
            Connection  connection = DriverManager.getConnection("jdbc:calcite:", info);

            Statement statement = connection.createStatement();

            test1(statement);

    }

    static void dealCsv(String model,String sql) throws SQLException, IOException {
        Properties info = new Properties();
        info.put("model", model);
        Connection  connection = DriverManager.getConnection("jdbc:calcite:", info);
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);
        JSONObject jsonObject = JSON.parseObject(FileUtils.readFileToString(new File(model)));
        String schemas = jsonObject.getJSONArray("schemas").getJSONObject(0).getJSONObject("operand").getString("schema");
        String line = FileUtils.readFileToString(new File(schemas));
        JSONArray jsonArray = JSON.parseObject(line).getJSONArray("columns");
        List<Columns> columns = JSONObject.parseArray(jsonArray.toString(), Columns.class);
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnSize = metaData.getColumnCount();
        while (resultSet.next()) {
            Map<String, Object> map = Maps.newLinkedHashMap();
            for (int i = 1; i < columnSize + 1; i++) {
                int finalI = i;
                columns.stream().forEach(column -> {
                    try {
                        if (column.getName().toLowerCase().equals(metaData.getColumnLabel(finalI))){

                            map.put(column.getName(), resultSet.getObject(finalI));
                        }
                    } catch (SQLException throwables) {
                        throwables.printStackTrace();
                    }
                });

            }
            System.out.println(JSONObject.toJSONString(map));
        }
    }

    /**
     * CSV文件读取
     * @param statement
     * @throws Exception
     */
    public static void test1(Statement statement) throws Exception {
        getData(statement.executeQuery(" select * from test where age > 20"));
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