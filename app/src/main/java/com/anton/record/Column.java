package com.anton.record;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
// column of a table
public class Column implements Serializable {
  private final String name;
  private final DataType type; // 'INT', 'STRING' etc.
}
