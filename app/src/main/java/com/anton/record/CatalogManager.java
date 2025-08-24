package com.anton.record;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// Responsible for storing meta data for our database
public class CatalogManager {
  private final File catalogFile;
  private Map<String, Table> tables = new HashMap<>();

  // default file path
  public CatalogManager() throws IOException {
    this("storage/catalog.db");
  }

  public CatalogManager(String catalogFilePath) throws IOException {
    this.catalogFile = new File(catalogFilePath);
    if (this.catalogFile.exists()) {
      loadCatalog();
    } else {
      saveCatalog(); // create empty catalog file
    }
  }

  // synchronized -> only one thread can execute this function at a time, to avoid inconsistensies
  public synchronized Table createTable(String tableName, List<Column> columns) throws IOException {
    if (tables.containsKey(tableName)) {
      throw new RuntimeException("Table already exists: " + tableName);
    }

    String fileName = "storage/" + tableName.toLowerCase() + ".tbl";
    // create the table file
    new File(fileName).createNewFile();

    Table schema = new Table(tableName.toLowerCase(), columns, fileName);
    tables.put(tableName, schema);
    saveCatalog();

    return schema;
  }

  public synchronized void insertTuple(String tableName, Tuple tuple) throws IOException {
    Table table = this.tables.get(tableName);
    if (table == null) {
      throw new IllegalArgumentException("Table does not exist: " + tableName);
    }
    table.insert(tuple);
    saveCatalog();
  }

  public synchronized List<Tuple> selectTuples(String tableName) throws IOException {
    Table table = this.tables.get(tableName);
    if (table == null) {
      throw new IllegalArgumentException("Table does not exist: " + tableName);
    }
    return table.selectAll();
  }

  public synchronized Table getTableSchema(String tableName) {
    return tables.get(tableName);
  }

  public synchronized List<String> listTables() {
    return new ArrayList<>(tables.keySet());
  }

  public synchronized void dropTable(String tableName) throws IOException {
    Table table = tables.remove(tableName);
    // Delete the table file associated with the dropped table
    if (table != null) {
      File file = new File(table.getFileName());
      if (file.exists()) {
        file.delete();
      }
      saveCatalog();
    }
  }

  // ========== Persistance ========== \\

  // Convert runtime Tables -> serializable TableEntry list
  private void saveCatalog() throws IOException {
    List<TableEntry> entries = new ArrayList<>();
    for (Table t : this.tables.values()) {
      entries.add(new TableEntry(t.getTableName(), t.getFileName(), t.getColumns(), t.getTupleIds()));
    }

    try (
      FileOutputStream fos = new FileOutputStream(this.catalogFile);
      ObjectOutputStream oos = new ObjectOutputStream(fos);
    ) {
      oos.writeObject(entries);
      oos.flush();
      // fsync
      FileDescriptor fd = fos.getFD();
      fd.sync();
    } catch (Exception e) {
      throw new IOException("Failed to save catalog. E:" + e);
    }
  }

  @SuppressWarnings("unchecked")
  private void loadCatalog() throws IOException {
    List<TableEntry> entries;
    try (
      ObjectInputStream ois = new ObjectInputStream(new FileInputStream(this.catalogFile))
    ) {
      entries = (List<TableEntry>) ois.readObject();
    } catch (Exception e) {
      throw new IOException("Failed to load catalog. E:" + e);
    }

    // Rebuild runtime tables from entries
    this.tables.clear();
    for (TableEntry e : entries) {
      Table table = new Table(e.getTableName(), e.getColumns(), e.getFileName(), e.getTupleIds());
      this.tables.put(table.getTableName(), table);
    }
  }
}
