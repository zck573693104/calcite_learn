package org.apache.calcite;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import lombok.Data;
import org.apache.calcite.csv.CsvUtil;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;


public class Client {

    @Data
    private static class OriginData{

        public OriginData() {
        }

        public OriginData(String schemaPath, String dataPath, String sinkPath) {
            this.schemaPath = schemaPath;
            this.dataPath = dataPath;
            this.sinkPath = sinkPath;
        }

        //记录列的信息
        private String schemaPath;
        //数据 csv的地址
        private String dataPath;
        //多个csv与schema合并之后路径
        private String sinkPath;
    }

    @Data
    private static class SqlData{

        public SqlData() {
        }

        public SqlData(String modelPath, String sql, String sinkPath, int threshold) {
            this.modelPath = modelPath;
            this.sql = sql;
            this.sinkPath = sinkPath;
            this.threshold = threshold;
        }

        //calcite model的路径
        private String modelPath;
        // 逻辑sql
        private String sql;
        //最后生成文件路径
        private String sinkPath;
        //多少数据写一次文件
        private int threshold;
    }

    public static void dealModel(List<OriginData> originDataList,SqlData sqlData) throws SQLException, IOException {
        originDataList.forEach(originData -> {
            CsvUtil.mergeFile(originData.getSchemaPath(), originData.getDataPath(), originData.getSinkPath());
        });
        dealCsv(sqlData);
    }
    /**
     * 测试的时候用字符串 defaultSchema 默认数据库 name 数据库名称 type custom factory
     * 请求接收类，该类会实例化Schema也就是数据库类，Schema会实例化Table实现类，Table会实例化数据类。
     * operand 动态参数，ScheamFactory的create方法会接收到这里的数据
     */
    public static void main(String[] args) throws Exception {
        List<OriginData> originDataList = new ArrayList<>();
        OriginData originData = new OriginData("/load/data/schema.json","/load/data/test1/test_1.csv","/load/data/sink/test_1.csv");

        OriginData originData1 = new OriginData("/load/data/schema.json","/load/data/test2/test_2.csv","/load/data/sink/test_2.csv");
        originDataList.add(originData);
        originDataList.add(originData1);
        String sql = "select test_1.name,NULLIF(test_2.age,1)  from test_1 as test_1 left join test_2 as test_2 on test_1.name=test_2.name ";
        SqlData sqlData = new SqlData("/load/data/model.json",sql,"/load/data/test.json",100);
        dealModel(originDataList,sqlData);


    }

    public static void dealCsv(SqlData sqlData) throws SQLException, IOException {

        String model = sqlData.getModelPath();
        String sql = sqlData.getSql();
        String sinkPath = sqlData.getSinkPath();
        int threshold = sqlData.getThreshold();
        File sinkFile = new File(sinkPath);
        if (sinkFile.exists()) sinkFile.delete();
        Properties info = new Properties();
        List<String> resultList = new ArrayList<>();
        info.put("model", model);
        Connection connection = DriverManager.getConnection("jdbc:calcite:caseSensitive=false;unquotedCasing=UNCHANGED", info);
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);

        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnSize = metaData.getColumnCount();
        while (resultSet.next()) {

            Map<String, Object> map = Maps.newLinkedHashMap();
            for (int i = 1; i < columnSize + 1; i++) {
                map.put(metaData.getColumnName(i), resultSet.getObject(i));
            }
            if (resultList.size() < threshold) {
                resultList.add(JSONObject.toJSONString(map));
            } else {
                FileUtils.writeLines(sinkFile, "utf-8", resultList, true);
                resultList.clear();
                resultList.add(JSONObject.toJSONString(map));
            }

        }
        FileUtils.writeLines(sinkFile, "utf-8", resultList, true);
    }


}