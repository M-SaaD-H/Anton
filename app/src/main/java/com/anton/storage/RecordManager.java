package com.anton.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class RecordManager implements AutoCloseable {
  private final PageManager pageManager;

  public RecordManager(PageManager pageManager) {
    this.pageManager = pageManager;
  }

  public RecordId insertRecord(byte[] data) throws IOException {
    int numOfPages = pageManager.getNumOfPages();

    // try inserting the record in all the existing pages, will improve this later
    for (int i = 0; i < numOfPages; i++) {
      Page page = pageManager.getPage(i); // i -> pageNumber
      RecordId id = tryInsertingIntoPage(page, i, data);

      // if we found the required space in any of the page then update that page
      if (id != null) {
        pageManager.writePage(i, page);
        return id;
      }
    }

    // if required space is not found in any of the pages
    Page newPage = new Page();
    RecordId id = tryInsertingIntoPage(newPage, numOfPages, data);
    if (id == null) {
      throw new IllegalStateException("Record too large for a page");
    }
    pageManager.writePage(numOfPages, newPage);
    return id;
  }

  public byte[] readRecord(RecordId id) throws IOException {
    Page page = pageManager.getPage(id.getPageNumber());
    return readFromPage(page, id.getSlotIndex());
  }

  public void deleteRecord(RecordId id) throws IOException {
    Page page = pageManager.getPage(id.getPageNumber());
    int slotIndex = id.getSlotIndex();
    byte[] data = page.getData();
    ByteBuffer buffer = ByteBuffer.wrap(data);
    int recordCount = buffer.getInt(0);

    if (slotIndex < 0 || slotIndex >= recordCount) {
      throw new IllegalArgumentException("Invalid slot index");
    }

    // Find the offset of the record to delete
    int offset = 4;
    for (int i = 0; i < slotIndex; i++) {
      int recordLength = buffer.getInt(offset);
      offset += 4 + recordLength;
    }

    int recordLengthToDelete = buffer.getInt(offset);
    int recordTotalLength = 4 + recordLengthToDelete;
    int nextRecordOffset = offset + recordTotalLength;

    // Calculate the number of bytes to move
    int usedBytes = nextRecordOffset;
    for (int i = slotIndex + 1; i < recordCount; i++) {
      int recordLength = buffer.getInt(usedBytes);
      usedBytes += 4 + recordLength;
    }

    int bytesToMove = usedBytes - nextRecordOffset;

    // Shift the remaining records left to fill the gap
    if (bytesToMove > 0) {
      System.arraycopy(data, nextRecordOffset, data, offset, bytesToMove);
    }

    // Zero out the leftover bytes at the end
    Arrays.fill(data, usedBytes - recordTotalLength, usedBytes, (byte) 0);

    // Update the record count
    buffer.putInt(0, recordCount - 1);

    // No need to update slot indices in RecordId objects, as they are not stored in the page
    // The caller should discard the deleted RecordId

    // Write the updated page back to storage
    pageManager.writePage(id.getPageNumber(), page);
  }

  // try inserting data into the page if 
  public RecordId tryInsertingIntoPage(Page page, int pageNumber, byte[] data) {
    byte[] pageData = page.getData();
    ByteBuffer buffer = ByteBuffer.wrap(pageData); // buffer -> so we can read/write data without manually shifting the bytes
    int recordCount = buffer.getInt(0); // reads the int (4bytes) and get the no. of records stored in the page starting from the 0th (first) position

    // initial offset for the record count
    int offset = 4; // after the loop this will be the pointer after the last record
    for (int i = 1; i < recordCount; i++) {
      int recordLength = buffer.getInt(offset); // read the length of the record

      // 4 -> integer = length of the record, recordLength -> actual length of the record
      offset += 4 + recordLength; // moves the pointer till the end of the data of the page
    }

    // space check
    int requiredSpace = 4 + data.length;
    if (offset + requiredSpace > pageData.length) {
      return null; // not enough space
    }

    // Write a new Record
    buffer.position(offset); // sets the buffer's pointer at the end of the data in the page
    buffer.putInt(data.length); // stores the length of the data
    buffer.put(data); // stores the data in the page

    // update the record count at the begining of the page
    buffer.putInt(0, recordCount + 1);

    return new RecordId(pageNumber, recordCount);
  }

  public byte[] readFromPage(Page page, int slotIndex) {
    ByteBuffer buffer = ByteBuffer.wrap(page.getData());
    int recordCount = buffer.getInt(0);

    if (slotIndex < 0 || slotIndex >= recordCount) {
      throw new IllegalArgumentException("Invalid slot index");
    }

    int offset = 4;
    for (int i = 0; i < slotIndex; i++){
      int recordLength = buffer.getInt(offset);
      offset += 4 + recordLength;
    }

    // length of the record that we have to read
    int requiredRecordsLength = buffer.getInt(offset);
    offset += 4; // take the offset past the length (integer)

    byte[] data = new byte[requiredRecordsLength];
    buffer.position(offset);
    buffer.get(data); // reads the the next data.length bytes and stores it into the data
    return data;
  }

  @Override
  public void close() throws IOException {
    if (pageManager != null) {
      pageManager.close();
    }
  }
}
