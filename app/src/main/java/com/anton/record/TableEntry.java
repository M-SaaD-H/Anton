package com.anton.record;

import java.io.Serializable;
import java.util.List;

import com.anton.storage.RecordId;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
// To Serialize table data and persist in the disk
public class TableEntry implements Serializable {
  private final String tableName;
  private final String fileName;
  private final List<Column> columns;
  private final List<RecordId> tupleIds;
}
