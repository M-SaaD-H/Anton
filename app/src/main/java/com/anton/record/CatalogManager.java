package com.anton.record;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// Responsible for storing meta data for our database
// keeping it in memory as of now, have to serialize it later.
public class CatalogManager {
  private Map<String, Table> tables;

  public CatalogManager() {
    this.tables = new HashMap<>();
  }

  public Table createTable(String tableName, List<Column> columns) {
    if (tables.containsKey(tableName)) {
      throw new RuntimeException("Table already exists");
    }

    Table schema = new Table(tableName, columns, tableName.toLowerCase() + ".tbl");
    tables.put(tableName, schema);

    return schema;
  }

  public Table getTableSchema(String tableName) {
    return tables.get(tableName);
  }

  public List<String> listTables() {
    return new ArrayList<>(tables.keySet());
  }

  public void dropTable(String tableName) {
    tables.remove(tableName);
    // Delete the table file associated with the dropped table
    File file = new File(tableName.toLowerCase() + ".tbl");
    if (file.exists()) {
      file.delete();
    }
  }
}
