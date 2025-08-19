package com.anton.record;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.anton.storage.RecordId;

public class TupleTest {
  @Test
  void testInsertTuple() throws Exception {
    List<Column> cols = List.of(
        new Column("id", DataType.INT),
        new Column("name", DataType.STRING));

    Table table = new Table("students", cols, "students.tbl");

    Map<String, Object> values = Map.of("id", 1, "name", "Alice");
    Tuple tuple = new Tuple(values);

    RecordId id = table.insert(tuple);
    assertNotNull(id);

    Tuple readTuple = table.read(id);
    assertTrue(readTuple != null); // simple sanity check
  }
}
