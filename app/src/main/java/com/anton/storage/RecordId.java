package com.anton.storage;

// details to find the Record
public class RecordId {
  private final int pageNumber;
  private final int slotIndex; // index of the record in the page

  public RecordId(int pageNumber, int slotIndex) {
    this.pageNumber = pageNumber;
    this.slotIndex = slotIndex;
  }

  public int getPageNumber() {
    return this.pageNumber;
  }

  public int getSlotIndex() {
    return this.slotIndex;
  }
}
