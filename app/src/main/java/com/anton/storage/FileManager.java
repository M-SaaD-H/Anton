package com.anton.storage;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

public class FileManager implements AutoCloseable {
  private RandomAccessFile raf;
  private volatile boolean isClosed = false;

  public FileManager(String filePath) throws IOException {
    File file = new File(filePath);
    this.raf = new RandomAccessFile(file, "rw");
  }

  // reads length bytes of data from the file starting from the current file pointer
  public byte[] read(long position, int length) throws IOException {
    raf.seek(position); // sets the file pointer to read and write the file
    byte[] buffer = new byte[length];
    raf.readFully(buffer);
    return buffer;
  }

  // write the bytes (b) to the file stating from the file pointer
  public void write(byte[] b, long position) throws IOException {
    raf.seek(position);
    raf.write(b);
  }

  public long getFileLength() throws IOException {
    return raf.length();
  }

  // close the file -> no read and write will be functional after this
  @Override
  public synchronized void close() throws IOException {
    if (isClosed || raf == null) {
      return; // Already closed
    }

    try {
      // Flush any buffered data before closing
      raf.getFD().sync();
    } catch (IOException e) {
      System.err.println("Warning: Failed to sync file before closing: " + e.getMessage());
      // Continue with close even if sync fails
    } finally {
      try {
        this.raf.close();
      } finally {
        this.raf = null;
        this.isClosed = true;
      }
    }
  }

  public boolean isClosed() {
    return this.isClosed;
  }
}
