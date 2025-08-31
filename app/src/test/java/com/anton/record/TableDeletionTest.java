package com.anton.record;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class TableDeletionTest {
  public static void main(String[] args) {
    try {
      CatalogManager catalog = new CatalogManager("test_catalog.db");
      
      // Create two tables
      System.out.println("Creating table1...");
      catalog.createTable("table1", Arrays.asList(
        new Column("id", DataType.INT),
        new Column("name", DataType.STRING)
      ));
      
      System.out.println("Creating table2...");
      catalog.createTable("table2", Arrays.asList(
        new Column("id", DataType.INT),
        new Column("value", DataType.STRING)
      ));
      
      // Insert some data
      Map<String, Object> data1 = new HashMap<>();
      data1.put("id", 1);
      data1.put("name", "test1");
      catalog.insertTuple("table1", new Tuple(data1));
      
      Map<String, Object> data2 = new HashMap<>();
      data2.put("id", 2);
      data2.put("value", "test2");
      catalog.insertTuple("table2", new Tuple(data2));
      
      System.out.println("Tables created and data inserted successfully.");
      System.out.println("Available tables: " + catalog.listTables());
      
      // Delete first table
      System.out.println("\nDeleting table1...");
      catalog.dropTable("table1");
      System.out.println("table1 deleted successfully.");
      System.out.println("Available tables: " + catalog.listTables());
      
      // Delete second table
      System.out.println("\nDeleting table2...");
      catalog.dropTable("table2");
      System.out.println("table2 deleted successfully.");
      System.out.println("Available tables: " + catalog.listTables());
      System.out.println("\nTest completed successfully!");  
    } catch (Exception e) {
      System.err.println("Error: " + e.getMessage());
      e.printStackTrace();
    }
  }
}
