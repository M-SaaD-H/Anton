package com.anton.record;

import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;
import java.io.File;

class TableDeletionTest {
  private static final String CATALOG_FILE = "test_catalog.db";

  @BeforeEach
  void cleanupBefore() {
    File f = new File(CATALOG_FILE);
    if (f.exists()) f.delete();
    File t1 = new File("storage/table1.tbl");
    if (t1.exists()) t1.delete();
    File t2 = new File("storage/table2.tbl");
    if (t2.exists()) t2.delete();
  }

  @Test
  void testTableDeletion() throws Exception {
    CatalogManager catalog = new CatalogManager(CATALOG_FILE);

    // Create two tables
    catalog.createTable("table1", java.util.Arrays.asList(
      new Column("id", DataType.INT),
      new Column("name", DataType.STRING)
    ));
    catalog.createTable("table2", java.util.Arrays.asList(
      new Column("id", DataType.INT),
      new Column("value", DataType.STRING)
    ));

    // Insert some data
    java.util.Map<String, Object> data1 = new java.util.HashMap<>();
    data1.put("id", 1);
    data1.put("name", "test1");
    catalog.insertTuple("table1", new Tuple(data1));

    java.util.Map<String, Object> data2 = new java.util.HashMap<>();
    data2.put("id", 2);
    data2.put("value", "test2");
    catalog.insertTuple("table2", new Tuple(data2));

    assertTrue(catalog.listTables().contains("table1"));
    assertTrue(catalog.listTables().contains("table2"));

    // Delete first table
    catalog.dropTable("table1");
    assertFalse(catalog.listTables().contains("table1"));
    assertTrue(new File("storage/table1.tbl").exists() == false);

    // Delete second table
    catalog.dropTable("table2");
    assertFalse(catalog.listTables().contains("table2"));
    assertTrue(new File("storage/table2.tbl").exists() == false);
  }

  @AfterEach
  void cleanupAfter() {
    File f = new File(CATALOG_FILE);
    if (f.exists()) f.delete();
    File t1 = new File("storage/table1.tbl");
    if (t1.exists()) t1.delete();
    File t2 = new File("storage/table2.tbl");
    if (t2.exists()) t2.delete();
  }
}
