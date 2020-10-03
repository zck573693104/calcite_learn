package org.apache.calcite.util;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class FileUtil {

    public static List<File> getFiles(File file, String suffix) {
        List<File> fileList = new ArrayList<>();

        if (file.isDirectory()) {
            File[] files = file.listFiles();
            for (File fileIndex : files) {
                if (fileIndex.isDirectory()) {
                    getFiles(fileIndex, suffix);
                } else if (suffix != null && fileIndex.getName().contains(suffix)) {
                    fileList.add(fileIndex);
                }
            }
        } else if (suffix != null && file.getName().contains(suffix)) {
            fileList.add(file);

        }
        return fileList;
    }

}
