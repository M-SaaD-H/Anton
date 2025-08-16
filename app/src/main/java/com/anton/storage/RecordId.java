package com.anton.storage;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
// details to find the Record
public class RecordId {
  private int pageNumber;
  private int slotIndex; // index of the record in the page

  public RecordId(int pageNumber, int slotIndex) {
    this.pageNumber = pageNumber;
    this.slotIndex = slotIndex;
  }
}
