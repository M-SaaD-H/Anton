package com.anton.storage;

import java.io.IOException;

// fix sized chunk of data which will be read and written as a whole
public class Page {
  private static final int PAGE_SIZE = 4096; // 4KB
  private final byte[] data;

  public Page() {
    this.data = new byte[PAGE_SIZE];
  }

  public byte[] getData() {
    return this.data;
  }

  public void setData(byte[] data) {
    if (data.length > PAGE_SIZE) {
      throw new IllegalArgumentException("E: Data exceeds page size limit");
    }

    System.arraycopy(data, 0, this.data, 0, data.length);

    // this.data = data;
    // this one just changes the pointer of of this.data to the new data, but it is better to just copy the data into its original data array.
  }

  public void readFromFile(FileManager fileManager, int pageNumber) throws IOException {
    long offset = (long) pageNumber * PAGE_SIZE;
    byte[] buffer = fileManager.read(offset, this.data.length);
    System.arraycopy(buffer, 0, this.data, 0, buffer.length);
  }

  public void writeToFile(FileManager fileManager, int pageNumber) throws IOException {
    long offset = (long) pageNumber * PAGE_SIZE;
    fileManager.write(this.data, offset);
  }
}
