package org.apache.calcite.csv;

import com.alibaba.fastjson.JSONObject;
import org.apache.calcite.Columns;
import org.apache.calcite.util.FileUtil;
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class CsvUtil {

    public static void mergeFile(String schemaPath, String dataPath, String sinkPath) {
        try {
            File sinkFile = new File(sinkPath);
            if (sinkFile.exists()) sinkFile.delete();
            String schema = FileUtils.readFileToString(new File(schemaPath));
            List<Columns> columnsList = JSONObject.parseArray(JSONObject.parseObject(schema).getJSONArray("columns").toString(), Columns.class);
            StringBuilder sb = new StringBuilder();
            columnsList.forEach(columns -> {
                sb.append(columns.getName());
                sb.append(":");
                sb.append(columns.getType());
                sb.append(",");
            });
            sb.deleteCharAt(sb.length() - 1);
            List<String> schemaList = new ArrayList<>(1);
            schemaList.add(sb.toString());
            FileUtils.writeLines(sinkFile, schemaList);
            List<File> fileList = FileUtil.getFiles(new File(dataPath),"csv");

            for (File file : fileList) {
                FileUtils.writeLines(sinkFile, "utf-8", FileUtils.readLines(file), true);
            }

        } catch (IOException e) {
            // File对象的创建过程中的异常捕获
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        mergeFile("/load/data/schema.json", "/load/data/", "/load/data/sink/test_1.csv");
    }

}

