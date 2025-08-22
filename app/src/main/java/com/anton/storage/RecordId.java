package com.anton.storage;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
// details to find the Record
public class RecordId implements Serializable {
  private int pageNumber;
  private int slotIndex; // index of the record in the page
}
