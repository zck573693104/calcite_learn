package org.apache.calcite.csv;

import lombok.Data;

import java.io.File;
import java.io.Serializable;


@Data
public class CsvParam implements Serializable {

    public CsvParam(File directory, File schema) {
        this.directory = directory;
        this.schema = schema;
    }

    public CsvParam() {
    }

    //数据目录
    private File directory;
    //数据描述
    private File schema;


}
