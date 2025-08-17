package com.anton.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

// fix sized chunk of data which will be read and written as a whole
public class Page {
  private final int PAGE_SIZE = 4096; // 4KB
  private final byte[] data;
  private List<Slot> slots;
  private int freeSpacePointer;

  public Page() {
    this.data = new byte[PAGE_SIZE];
    this.slots = new ArrayList<>();
    this.freeSpacePointer = 0;
  }

  public byte[] getData() {
    return this.data;
  }

  public Slot getSlot(int slotIdx) {
    return this.slots.get(slotIdx);
  }

  public int getSlotsSize() {
    return this.slots.size();
  }

  public void addSlot(Slot slot) {
    this.slots.add(slot);
  }

  public void setData(byte[] data) {
    if (data.length > PAGE_SIZE) {
      throw new IllegalArgumentException("E: Data exceeds page size limit");
    }

    System.arraycopy(data, 0, this.data, 0, data.length);

    // this.data = data;
    // this one just changes the pointer of of this.data to the new data, but it is better to just copy the data into its original data array.
  }

  public int getFreeSpacePointer() {
    return this.freeSpacePointer;
  }

  public void setFreeSpacePointer(int fps) {
    this.freeSpacePointer = fps;
  }

  public void readFromFile(FileManager fileManager, int pageNumber) throws IOException {
    long offset = (long) pageNumber * PAGE_SIZE;
    byte[] pageData = fileManager.read(offset, PAGE_SIZE);
    ByteBuffer buffer = ByteBuffer.wrap(pageData);
    // Read slot count
    int slotCount = buffer.getInt();
    this.slots = new ArrayList<>();
    for (int i = 0; i < slotCount; i++) {
      int off = buffer.getInt();
      int len = buffer.getInt();
      slots.add(new Slot(off, len));
    }

    int slotDirSize = 4 + slotCount * 8;
    // Move buffer position to start of data region
    buffer.position(slotDirSize);

    int dataLength = PAGE_SIZE - slotDirSize;
    if (dataLength > 0) {
      buffer.get(this.data, 0, dataLength);
    }
    // Restore freeSpacePointer
    int maxEnd = 0;
    for (Slot s : slots) {
      if (s.getOffset() + s.getLength() > maxEnd) {
        maxEnd = s.getOffset() + s.getLength();
      }
    }
    this.freeSpacePointer = maxEnd;
  }

  public void writeToFile(FileManager fileManager, int pageNumber) throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(PAGE_SIZE);
    // Write slot count
    buffer.putInt(this.slots.size());
    // Write each slot's offset and length
    for (Slot slot : this.slots) {
      buffer.putInt(slot.getOffset());
      buffer.putInt(slot.getLength());
    }
    // Write the data array (after slot directory)
    int slotDirSize = 4 + this.slots.size() * 8; // 4 bytes for count, 8 bytes per slot
    int dataLength = Math.min(data.length, PAGE_SIZE - slotDirSize);
    // Fill up to slotDirSize with zeros if needed
    while (buffer.position() < slotDirSize) buffer.put((byte) 0);
    buffer.put(this.data, 0, dataLength);
    // Fill the rest with zeros if needed
    while (buffer.position() < PAGE_SIZE) buffer.put((byte) 0);
    fileManager.write(buffer.array(), (long) pageNumber * PAGE_SIZE);
  }

  public CompletableFuture<Void> compact() {
    return CompletableFuture.runAsync(() -> {
      this.compactPage();
    }).thenRun(() -> System.out.println("Compaction done!"));
  }

  private void compactPage() {
    // filter valid slots (separate deleted slots)
    List<Slot> validSlots = this.slots.stream().filter(s -> s.getLength() > 0).toList();
    // Sort slots by offset (to maintain correct copy order)
    validSlots.sort((a, b) -> Integer.compare(a.getOffset(), b.getOffset()));

    ByteBuffer buffer = ByteBuffer.wrap(this.data);

    // pointer to write from the begining
    int writePos = 0;
    for (Slot s : validSlots) {
      int offset = s.getOffset();
      int length = s.getLength();

      if (offset != writePos) {
        byte[] record = new byte[length];

        // move the record forward
        buffer.position(offset);
        buffer.get(record);

        buffer.position(writePos);
        buffer.put(record);

        // update the slot offset
        s.setOffset(writePos);
      }

      writePos += length;
    }

    // clear the leftover space at the end
    buffer.position(writePos);

    while (buffer.position() < this.PAGE_SIZE) {
      buffer.put((byte) 0);
    }

    // update the slot directory
    for (Slot s : this.slots) {
      // so that deleted slots do not point to any data
      if (s.getLength() == 0 || s.getOffset() < 0) {
        s.setLength(0);
        s.setOffset(-1);
      }
    }
  }
}
