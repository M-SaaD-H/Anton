package com.anton.storage;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
public class Slot {
  private int offset;
  private int length;
}
