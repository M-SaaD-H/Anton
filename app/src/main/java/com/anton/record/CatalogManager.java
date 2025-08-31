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
    // Ensure parent directory exists
    File parent = this.catalogFile.getParentFile();
    if (parent != null && !parent.exists()) {
      parent.mkdirs();
    }
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

  public synchronized List<Tuple> selectTuples(String tableName, Map<String, Object> condition, List<String> fields) throws IOException {
    Table table = this.tables.get(tableName);
    if (table == null) {
      throw new IllegalArgumentException("Table does not exist: " + tableName);
    }

    if (condition == null) {
      return table.selectAll(fields);
    }

    return table.select(condition, fields);
  }

  public synchronized Table getTableSchema(String tableName) {
    return tables.get(tableName);
  }

  public synchronized List<String> listTables() {
    return new ArrayList<>(tables.keySet());
  }

  public synchronized void dropTable(String tableName) throws IOException {
    Table table = this.tables.remove(tableName);
    if (table == null) {
      throw new RuntimeException("Table does not exist");
    }

    // Close all resources with proper error handling
    IOException closeException = null;
    try {
      // Ensure all file handles are properly closed
      table.close();
    } catch (IOException e) {
      closeException = e;
      System.err.println("Error occurred while closing the table. E: " + e.getMessage());
      e.printStackTrace();
    }

    // Garbage collection -> should not be used in production, but can't do much about it as of now
    System.gc();

    // Small delay to allow OS to release file handles
    // This is more predictable than System.gc() -> should not be used in production
    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    // Attempt file deletion with retry logic
    File file = new File(table.getFileName());
    if (file.exists()) {
      boolean deleted = deleteFileWithRetry(file);

      if (!deleted) {
        System.err.println("WARNING: Could not delete file: " + file.getAbsolutePath());
        file.deleteOnExit();
        System.err.println("Scheduled the deletion");
      } else {
        System.out.println("Successfully deleted file: " + file.getAbsolutePath());
      }
    } else {
      throw new RuntimeException("The table file does not exists.");
    }

    // Save catalog after successful cleanup
    try {
      saveCatalog();
    } catch (IOException e) {
      if (closeException != null) {
        e.addSuppressed(closeException);
      }
      throw e;
    }

    if (closeException != null) {
      throw closeException;
    }
  }

  private boolean deleteFileWithRetry(File file) {
    final int maxRetries = 5;
    final long[] delays = { 50, 100, 200, 500, 1000 }; // Progressive backoff

    for (int i = 0; i < maxRetries; i++) {
      if (file.delete()) {
        return true;
      }

      // Check why deletion failed
      if (!file.exists()) {
        return true; // File was deleted by another process
      }

      if (i < maxRetries - 1) {
        try {
          Thread.sleep(delays[i]);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return false;
        }
      }
    }

    return false;
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
