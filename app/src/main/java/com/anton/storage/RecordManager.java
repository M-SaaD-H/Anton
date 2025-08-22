package com.anton.storage;

import java.io.IOException;
import java.nio.ByteBuffer;

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
    Page newPage = pageManager.getPage(numOfPages); // this function will return a new page and will add it to the 'pageCache'. TODO: improve this
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

  public byte[] readFromPage(Page page, int slotIndex) {
    int slotSize = page.getSlotsSize();
    if (slotIndex < 0 || slotIndex >= slotSize) {
      throw new IllegalArgumentException("Invalid slot index");
    }

    if (slotSize <= 0) {
      throw new IllegalStateException("No slots found in this page");
    }

    Slot slot = page.getSlot(slotIndex);

    int offset = slot.getOffset();
    int length = slot.getLength();

    if (offset < 0 || length <= 0) {
      throw new IllegalStateException("Slot is empty or deleted");
    }

    ByteBuffer buffer = ByteBuffer.wrap(page.getData());

    byte[] data = new byte[length];
    buffer.position(offset);
    buffer.get(data); // reads the the next data.length bytes and stores it into the data
    return data;
  }

  public void deleteRecord(RecordId id) throws IOException {
    Page page = pageManager.getPage(id.getPageNumber());
    int slotIndex = id.getSlotIndex();

    if (slotIndex < 0) {
      throw new IllegalArgumentException("Invalid slot index");
    }

    Slot slot = page.getSlot(slotIndex);
    slot.setLength(0); // mark deleted, not changing the offset -> will be used to clear the memory

    // actually delete the record asynchronously
    page.compact();
    // write the updated page
    pageManager.writePage(id.getPageNumber(), page);
  }

  // try inserting data into the page if 
  public RecordId tryInsertingIntoPage(Page page, int pageNumber, byte[] data) {
    byte[] pageData = page.getData();
    ByteBuffer buffer = ByteBuffer.wrap(pageData); // buffer -> so we can read/write data without manually shifting the bytes
    int fps = page.getFreeSpacePointer();

    // space check
    if (fps + data.length > pageData.length) {
      return null; // not enough space
    }

    // Write a new Record
    buffer.position(fps); // sets the buffer's pointer at the end of the data in the page
    buffer.put(data); // stores the data in the page

    Slot slot = new Slot(fps, data.length);
    page.addSlot(slot);

    return new RecordId(pageNumber, page.getSlotsSize() - 1);
  }

  @Override
  public void close() throws IOException {
    if (pageManager != null) {
      pageManager.close();
    }
  }
}
