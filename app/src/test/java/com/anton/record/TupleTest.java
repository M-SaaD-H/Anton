package com.anton.record;

import static org.junit.Assert.assertNotNull;
import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.*;

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
    assertNotNull(readTuple); // simple sanity check
  }

  @Test
  void testDeleteTuple() throws Exception {
    List<Column> cols = List.of(
        new Column("id", DataType.INT),
        new Column("name", DataType.STRING));
    Table table = new Table("students", cols, "students.tbl");
    Map<String, Object> values = Map.of("id", 2, "name", "Bob");
    Tuple tuple = new Tuple(values);
    RecordId id = table.insert(tuple);
    assertNotNull(id);
    // Delete the tuple
    java.util.Map<String, Object> cond = Map.of("id", 2);
    table.delete(cond);
    // After deletion, reading should throw or return null
    assertThrows(Exception.class, () -> table.read(id));
    // tupleIds should not contain the deleted id
    assertFalse(table.getTupleIds().contains(id));
  }
}
