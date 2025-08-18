package com.anton.record;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
// column of a table
public class Column {
  private String name;
  private String type; // 'INT', 'STRING' etc.
}
