package com.anton.storage;

import org.junit.jupiter.api.*;
import java.io.*;

import static org.junit.jupiter.api.Assertions.*;

public class PageTest {
    private File tempFile;
    private FileManager fileManager;

    @BeforeEach
    void setup() throws IOException {
        tempFile = File.createTempFile("testdb", ".db");
        fileManager = new FileManager(tempFile.getAbsolutePath());
    }

    @AfterEach
    void cleanup() throws IOException {
        fileManager.close();
        tempFile.delete();
    }

    @Test
    void testPageReadWrite() throws IOException {
        Page page = new Page();
        byte[] data = "Hello, DB!".getBytes();
        page.setData(data);

        page.writeToFile(fileManager, 0);

        Page readPage = new Page();
        readPage.readFromFile(fileManager, 0);

        assertEquals("Hello, DB!", new String(readPage.getData(), 0, data.length));
    }
}
