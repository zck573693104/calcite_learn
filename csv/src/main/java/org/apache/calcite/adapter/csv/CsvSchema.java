/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.csv;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.adapter.file.JsonScannableTable;
import org.apache.calcite.csv.CsvParam;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Schema mapped onto a directory of CSV files. Each table in the schema
 * is a CSV file in that directory.
 */
public class CsvSchema extends AbstractSchema {
  private final CsvParam csvParam;
  private final CsvTable.Flavor flavor;
  private Map<String, Table> tableMap;

  /**
   * Creates a CSV schema.
   *
   * @param csvParam Directory that holds {@code .csv} files
   * @param flavor     Whether to instantiate flavor tables that undergo
   *                   query optimization
   */
  public CsvSchema(CsvParam csvParam, CsvTable.Flavor flavor) {
    super();
    this.csvParam = csvParam;
    this.flavor = flavor;
  }

  /** Looks for a suffix on a string and returns
   * either the string with the suffix removed
   * or the original string. */
  private static String trim(String s, String suffix) {
    String trimmed = trimOrNull(s, suffix);
    return trimmed != null ? trimmed : s;
  }

  /** Looks for a suffix on a string and returns
   * either the string with the suffix removed
   * or null. */
  private static String trimOrNull(String s, String suffix) {
    return s.endsWith(suffix)
        ? s.substring(0, s.length() - suffix.length())
        : null;
  }

  @Override protected Map<String, Table> getTableMap() {
    if (tableMap == null) {
      tableMap = createTableMap();
    }
    return tableMap;
  }

  public static List<File> getFiles(File file) {
    List<File> fileList = new ArrayList<>();

    if (file.isDirectory()) {
      File[] files = file.listFiles();
      for (File fileIndex : files) {
        if (fileIndex.isDirectory()) {
          getFiles(fileIndex);
        } else {
          fileList.add(fileIndex);
        }
      }
    } else {
      fileList.add(file);
    }
    return fileList;
  }

  private Map<String, Table> createTableMap() {
    // Look for files in the directory ending in ".csv", ".csv.gz", ".json",
    // ".json.gz".
    final Source baseSource = Sources.of(csvParam.getDirectory());
    List<File> files = getFiles(csvParam.getDirectory()).stream().filter( file -> {
      final String nameSansGz = trim(file.getName(), ".gz");
      return nameSansGz.endsWith(".csv")
              || nameSansGz.endsWith(".json");
    }).collect(Collectors.toList());

    // Build a map from table name to table; each file becomes a table.
    final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
    for (File file : files) {
      Source source = Sources.of(file);
      Source sourceSansGz = source.trim(".gz");
      final Source sourceSansJson = sourceSansGz.trimOrNull(".json");
      if (sourceSansJson != null) {
        final Table table = new JsonScannableTable(source);
        builder.put(sourceSansJson.relative(baseSource).path(), table);
      }
      final Source sourceSansCsv = sourceSansGz.trimOrNull(".csv");
      if (sourceSansCsv != null) {
        final Table table = createTable(source,csvParam);
        String tableName = file.getName().substring(0,file.getName().lastIndexOf("."));
        builder.put(tableName.toUpperCase(), table);
      }
    }
    return builder.build();
  }

  /** Creates different sub-type of table based on the "flavor" attribute. */
  private Table createTable(Source source, CsvParam csvParam) {
    switch (flavor) {
    case TRANSLATABLE:
      return new CsvTranslatableTable(source, null);
    case SCANNABLE:
      return new CsvScannableTable(csvParam,source, null);
    case FILTERABLE:
      return new CsvFilterableTable(source, null);
    default:
      throw new AssertionError("Unknown flavor " + this.flavor);
    }
  }
}
