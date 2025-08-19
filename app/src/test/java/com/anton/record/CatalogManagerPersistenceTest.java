package com.anton.record;

import org.junit.jupiter.api.*;

import com.anton.storage.RecordId;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class CatalogManagerPersistenceTest {
  private static final String CATALOG_FILE = "test_catalog.db";

  @BeforeEach
  void cleanupBefore() {
    // Ensure clean file before each run
    java.io.File f = new java.io.File(CATALOG_FILE);
    if (f.exists())
      f.delete();
  }

  @Test
  void testCreatePersistReload() throws Exception {
    RecordId savedId;

    // 1) Start app → new CatalogManager
    CatalogManager cm1 = new CatalogManager(CATALOG_FILE);

    // 2) Create table students(id INT, name STRING)
    Table students = cm1.createTable(
      "students",
      List.of(
        new Column("id", DataType.INT),
        new Column("name", DataType.STRING)
      )
    );

    // 3) Insert tuple
    Tuple t = new Tuple(Map.of("id", 123, "name", "Alice"));
    savedId = students.insert(t);

    // 4) Simulate JVM restart → create new CatalogManager
    cm1 = null; // drop reference
    CatalogManager cm2 = new CatalogManager(CATALOG_FILE);

    // 5) Check schema still exists
    Table reloaded = cm2.getTableSchema("students");
    assertNotNull(reloaded, "students table should still exist after reload");

    // 6) Read the tuple back with the old RecordId
    Tuple readBack = reloaded.read(savedId);
    assertNotNull(readBack, "tuple should not be null after reload");
    assertEquals(123, readBack.getValue("id"));
    assertEquals("Alice", readBack.getValue("name"));
  }
}
