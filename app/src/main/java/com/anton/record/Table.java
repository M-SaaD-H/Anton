package com.anton.record;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
public class Table {
  private final String tableName;
  private List<Column> columns;
  private final String fileName; // reference to the file storing this table's data
}
