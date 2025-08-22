package com.anton.record;

import com.anton.sql.QueryExecutor;
import org.junit.jupiter.api.*;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

public class QueryParserTest {
  private static CatalogManager db;
  private static QueryExecutor executor;

  @BeforeAll
  static void setup() throws IOException {
    db = new CatalogManager("test_catalog.db");
    executor = new QueryExecutor(db);
  }

  @BeforeEach
  void clean() throws IOException {
    // drop any old tables before each test
    for (String table : db.listTables()) {
      db.dropTable(table);
    }
  }

  @Test
  void testCreateTable() throws Exception {
    String sql = "CREATE TABLE users (id INT, name STRING)";
    executor.execute(sql);

    assertTrue(db.listTables().contains("users"), "Table 'users' should exist");
  }

  @Test
  void testInsertAndSelect() throws Exception {
    // create table
    executor.execute("CREATE TABLE users (id INT, name STRING)");

    // insert rows
    executor.execute("INSERT INTO users VALUES ('id' 1, 'name' 'Saad')");
    executor.execute("INSERT INTO users VALUES ('id' 2, 'name' 'Anton')");

    // select all
    var result = executor.execute("SELECT * FROM users");

    assertNotNull(result);
    assertEquals(2, result.size());

    // row 1
    Tuple row1 = result.get(0);
    assertEquals(1, row1.getValue("id"));
    assertEquals("Saad", row1.getValue("name"));

    // row 2
    Tuple row2 = result.get(1);
    assertEquals(2, row2.getValue("id"));
    assertEquals("Anton", row2.getValue("name"));
  }

  @AfterAll
  static void tearDown() {
    // delete test catalog file
    java.io.File f = new java.io.File("test_catalog.db");
    if (f.exists()) {
      f.delete();
    }
  }
}
