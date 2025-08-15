package com.anton.storage;

// in-memory representation of record (data)
public class Record {
  private final byte[] data;
  
  public Record(byte[] data) {
    this.data = data;
  }

  public byte[] getData() {
    return this.data;
  }
}
