package org.apache.calcite;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;

import java.sql.*;
import java.util.Map;
import java.util.Properties;

public class HrSchema {

    public static class MyIncrement {
        public Object eval(Object x, Object y) {
            return x ==null || StringUtils.isBlank(x.toString())?y:x;
        }
    }

    public static void main(String []args) throws Exception {
        test();
    }
    public static void test() throws Exception {
        Properties info = new Properties();
        info.put("model", "/load/data/model.json");
        Connection connection = DriverManager.getConnection("jdbc:calcite:caseSensitive=false;unquotedCasing=UNCHANGED", info);

        Statement statement = connection.createStatement();
        String sql = "select " +
                " case test_1.name when 'false' then 0 " +
                "when 'true' then 1 else test_1.name  end as name "+
                ",IFNULL(test_2.age,UUID()) ,DATE_FORMAT(NOW(),'yyyy-MM-dd HH:mm:ss') from test_1 as test_1 left join test_2 as test_2 on test_1.name=test_2.name ";
        ResultSet resultSet = statement.executeQuery(sql);
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnSize = metaData.getColumnCount();
        while (resultSet.next()){
            Map<String, Object> map = Maps.newLinkedHashMap();
            for (int i = 1; i < columnSize + 1; i++) {

                map.put(metaData.getColumnName(i), resultSet.getObject(i));
            }
            System.out.println(JSONObject.toJSONString(map));
        }
        resultSet.close();
        statement.close();
        connection.close();
    }

}
