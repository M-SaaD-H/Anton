package com.anton.sql;

import java.util.List;
import java.util.Map;

import lombok.Getter;

@Getter
public abstract class Query {
  private final QueryType type;
  private final String tableName;

  public Query(QueryType type, String tableName) {
    this.type = type;
    this.tableName = tableName;
  }
}

enum QueryType {
  CREATE_TABLE, SELECT, INSERT, UPDATE, DELETE, DROP_TABLE
}

@Getter
class CreateTableQuery extends Query {
  private final Map<String, String> columns; // data type is accepted as a String, and will be casted in DataType while creating the table
  public CreateTableQuery(String tableName, Map<String, String> columns) {
    super(QueryType.CREATE_TABLE, tableName);
    this.columns = columns;
  }
}

@Getter
class SelectQuery extends Query {
  private final List<String> fields;
  private final Map<String, Object> conditions;
  public SelectQuery(String tableName, List<String> fields, Map<String, Object> conditions) {
    super(QueryType.SELECT, tableName);
    this.fields = fields;
    this.conditions = conditions;
  }
}

@Getter
class InsertQuery extends Query {
  private final Map<String, Object> values;
  public InsertQuery(String tableName, Map<String, Object> values) {
    super(QueryType.INSERT, tableName);
    this.values = values;
  }
}

@Getter
class UpdateQuery extends Query {
  private final Map<String, Object> values;
  private final Map<String, Object> conditions;
  public UpdateQuery(String tableName, Map<String, Object> values, Map<String, Object> conditions) {
    super(QueryType.UPDATE, tableName);
    this.values = values;
    this.conditions = conditions;
  }
}

@Getter
class DeleteQuery extends Query {
  private final Map<String, Object> conditions;
  public DeleteQuery(String tableName, Map<String, Object> conditions) {
    super(QueryType.DELETE, tableName);
    this.conditions = conditions;
  }
}

class DropTableQuery extends Query {
  public DropTableQuery(String tableName) {
    super(QueryType.DROP_TABLE, tableName);
  }
}