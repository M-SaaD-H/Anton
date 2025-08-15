package com.anton.storage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

public class RecordManagerTest {
  private RecordManager recordManager;
  private File testFile;

  @BeforeEach
  void setUp() throws IOException {
    testFile = new File("test_records.db");
    if (testFile.exists()) {
      testFile.delete();
    }
    FileManager fileManager = new FileManager("test_records.db");
    PageManager pageManager = new PageManager(fileManager);
    recordManager = new RecordManager(pageManager);
  }

  @AfterEach
  void tearDown() throws IOException {
    recordManager.close();
    if (testFile.exists()) {
      testFile.delete();
    }
  }

  @Test
  void testInsertAndRetrieveRecord() throws IOException {
    byte[] data = "Hello, Database!".getBytes();
    RecordId rid = recordManager.insertRecord(data);

    assertNotNull(rid);

    byte[] retrieved = recordManager.readRecord(rid);
    assertTrue(Arrays.equals(data, retrieved));
  }

  // @Test
  // void testDeleteRecord() throws IOException {
  //   byte[] data = "Temporary Record".getBytes();
  //   RecordId rid = recordManager.insertRecord(data);

  //   assertNotNull(recordManager.getRecord(rid));

  //   recordManager.deleteRecord(rid);

  //   assertThrows(IOException.class, () -> recordManager.getRecord(rid));
  // }

  // @Test
  // void testMultipleInserts() throws IOException {
  //   byte[] data1 = "First".getBytes();
  //   byte[] data2 = "Second".getBytes();

  //   RecordId rid1 = recordManager.insertRecord(data1);
  //   RecordId rid2 = recordManager.insertRecord(data2);

  //   assertTrue(Arrays.equals(data1, recordManager.getRecord(rid1).getData()));
  //   assertTrue(Arrays.equals(data2, recordManager.getRecord(rid2).getData()));
  // }
}