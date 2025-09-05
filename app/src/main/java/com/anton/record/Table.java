package com.anton.record;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.anton.storage.FileManager;
import com.anton.storage.PageManager;
import com.anton.storage.RecordId;
import com.anton.storage.RecordManager;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Table {
  private final String tableName;
  private List<Column> columns;
  private final String fileName; // reference to the file storing this table's data
  private RecordManager recordManager;
  private List<RecordId> tupleIds;

  public Table(String tableName, List<Column> columns, String fileName) {
    this.tableName = tableName;
    this.columns = columns;
    this.fileName = fileName;
    this.tupleIds = new ArrayList<>();

    try {
      this.recordManager = new RecordManager(new PageManager(new FileManager(fileName)));
    } catch (Exception e) {
      throw new RuntimeException("Failed to initialize table. E:" + e);
    }
  }

  public Table(String tableName, List<Column> columns, String fileName, List<RecordId> tupleIds) {
    this.tableName = tableName;
    this.columns = columns;
    this.fileName = fileName;
    this.tupleIds = tupleIds;

    try {
      FileManager fileManager = new FileManager(fileName);
      PageManager pageManager = new PageManager(fileManager);
      this.recordManager = new RecordManager(pageManager);
    } catch (Exception e) {
      throw new RuntimeException("Failed to initialize table. E:" + e);
    }
  }

  public RecordId insert(byte[] data) throws IOException {
    RecordId id = this.recordManager.insertRecord(data);
    this.tupleIds.add(id);
    return id;
  }

  public RecordId insert(Tuple tuple) throws IOException {
    // serialize the data
    byte[] data = tuple.toBytes(this.columns);
    // store the serialized data
    RecordId id = this.recordManager.insertRecord(data);
    this.tupleIds.add(id);
    return id;
  }

  public Tuple read(RecordId id) throws IOException {
    byte[] data = this.recordManager.readRecord(id);
    // return the de-serialized data
    Tuple tuple = Tuple.fromBytes(data, this.columns);
    tuple.setId(id);
    return tuple;
  }

  private boolean delete(RecordId id) throws IOException {
    try {
      this.recordManager.deleteRecord(id);
      System.out.println("Successfully deleted the record with page number: " + id.getPageNumber() + " and slot index: " + id.getSlotIndex());
      this.tupleIds.remove(id);
      return true;
    } catch (Exception e) {
      System.out.println("Failed to delete the record with page number: " + id.getPageNumber() + " and slot index: " + id.getSlotIndex());
      e.printStackTrace();
      return false;
    }
  }

  public void delete(Map<String, Object> conditions) throws IOException {
    System.out.println("Before deletion, tupleIds size: " + this.tupleIds.size());
    List<Tuple> allTuples = this.selectAll(null);
    List<RecordId> idsToDelete = new ArrayList<>();

    for (Tuple t : allTuples) {
      boolean match = true;
      for (String field : conditions.keySet()) {
        Object tupleValue = t.getValue(field);
        Object conditionValue = conditions.get(field);
        if (tupleValue == null || !tupleValue.toString().equals(conditionValue.toString())) {
          match = false;
          break;
        }
      }

      // if all values match, then add it to the result list
      if (match) {
        idsToDelete.add(t.getId());
      }
    }
    
    for (RecordId id : idsToDelete) {
      this.delete(id);
    }
  }

  public List<Tuple> selectAll(List<String> fields) throws IOException {
    if (this.tupleIds == null) {
      throw new RuntimeException("No tuples found in table: " + this.tableName);
    }

    List<Tuple> tuples = new ArrayList<>();
    for (RecordId id : this.tupleIds) {
      tuples.add(read(id));
    }

    return projectRequiredFields(tuples, fields);
  }

  public List<Tuple> select(Map<String, Object> conditions, List<String> fields) throws IOException {
    List<Tuple> allTuples = this.selectAll(null);
    List<Tuple> tuples = new ArrayList<>();

    for (Tuple t : allTuples) {
      boolean match = true;
      for (String field : conditions.keySet()) {
        Object tupleValue = t.getValue(field);
        Object conditionValue = conditions.get(field);
        if (tupleValue == null || !tupleValue.toString().equals(conditionValue.toString())) {
          match = false;
          break;
        }
      }

      // if all values match, then add it to the result list
      if (match) {
        tuples.add(t);
      }
    }

    return projectRequiredFields(tuples, fields);
  }

  private List<Tuple> projectRequiredFields(List<Tuple> tuples, List<String> fields) {
    if (fields == null) return tuples;

    // project only the required fields
    List<Tuple> projected = new ArrayList<>();
    for (Tuple t : tuples) {
      Map<String, Object> filteredValues = new HashMap<>();
      for (String f : fields) {
        filteredValues.put(f, t.getValue(f));
      }
      projected.add(new Tuple(filteredValues));
    }

    return projected;
  }

  public void close() throws IOException {
    this.recordManager.close();
  }
}
