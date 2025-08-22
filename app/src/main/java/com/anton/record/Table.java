package com.anton.record;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
      FileManager fileManager = new FileManager(fileName);
      PageManager pageManager = new PageManager(fileManager);
      this.recordManager = new RecordManager(pageManager);
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
    return Tuple.fromBytes(data, this.columns);
  }

  public boolean delete(RecordId id) throws IOException {
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

  public List<Tuple> selectAll() throws IOException {
    if (this.tupleIds == null) {
      throw new RuntimeException("No tuples found in table: " + this.tableName);
    }

    List<Tuple> tuples = new ArrayList<>();
    for (RecordId id : this.tupleIds) {
      tuples.add(read(id));
    }
    return tuples;
  }

  public void close() throws IOException {
    this.recordManager.close();
  }
}
