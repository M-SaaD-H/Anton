package com.anton.storage;

import org.junit.jupiter.api.*;
import java.io.*;

import static org.junit.jupiter.api.Assertions.*;

class FileManagerTest {

  private File tempFile;
  private FileManager fileManager;

  @BeforeEach
  void setUp() throws Exception {
    // Create a temporary file for testing
    tempFile = File.createTempFile("filemanager_test", ".db");
    tempFile.deleteOnExit();

    // Assuming FileManager takes a String path, not a File
    fileManager = new FileManager(tempFile.getAbsolutePath());
  }

  @AfterEach
  void tearDown() throws Exception {
    if (fileManager != null) {
      fileManager.close();
    }
  }

  @Test
  void testWriteAndReadHello() throws Exception {
    // Write "Hello" at position 0
    fileManager.write("Hello".getBytes(), 0);

    // Read back from position 0
    byte[] buffer = fileManager.read(0, 5);

    assertEquals("Hello", new String(buffer));
  }
}
