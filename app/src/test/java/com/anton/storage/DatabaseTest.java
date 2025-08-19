package com.anton.storage;

import java.util.List;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;

import com.anton.record.CatalogManager;
import com.anton.record.Column;
import com.anton.record.DataType;
import com.anton.record.Table;

public class DatabaseTest {
  public static void main(String[] args) throws Exception {
    // Step 1: Setup Catalog
    CatalogManager catalog = new CatalogManager();
    List<Column> columns = new ArrayList<>();
    columns.add(new Column("id", DataType.INT));
    columns.add(new Column("name", DataType.STRING));
    Table table = catalog.createTable("students", columns);

    // Step 2: Setup RecordManager (with PageManager backing)
    FileManager fileManager = new FileManager(table.getFileName());
    PageManager pageManager = new PageManager(fileManager); // file-based page manager
    RecordManager recordManager = new RecordManager(pageManager);

    // Step 3: Serialize a record according to schema
    int idVal = 1;
    String nameVal = "Saad";

    byte[] recordBytes = serializeRecord(idVal, nameVal);

    // Step 4: Insert the record
    RecordId rid = recordManager.insertRecord(recordBytes);
    System.out.println("Inserted Record at: " + rid);

    // Step 5: Read back the record
    byte[] readBytes = recordManager.readRecord(rid);
    Object[] row = deserializeRecord(readBytes);

    System.out.println("Read Record: id=" + row[0] + ", name=" + row[1]);

    recordManager.close();
  }

  // Helper: convert values -> bytes
  private static byte[] serializeRecord(int id, String name) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    dos.writeInt(id);
    dos.writeUTF(name);
    return baos.toByteArray();
  }

  // Helper: convert bytes -> values
  private static Object[] deserializeRecord(byte[] data) throws IOException {
    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));
    int id = dis.readInt();
    String name = dis.readUTF();
    return new Object[] { id, name };
  }
}
