package com.anton.sql;

import java.util.List;
import java.util.Map;

import lombok.Getter;

enum QueryType {
  CREATE_TABLE, SELECT, INSERT, UPDATE, DELETE
}

@Getter
public class Query {
  private final QueryType type;
  private final String tableName;
  private final Map<String, Object> values; // for INSERT and UPDATE query
  private final List<String> fields; // for SELECT query
  private final Map<String, Object> condition; // condition for SELECT query

  // for INSERT, UPDATE, and CREATE TABLE query, and for DELETE query the values will empty
  public Query(QueryType type, String tableName, Map<String, Object> values) {
    if (type == null || tableName == null || values == null) {
      throw new IllegalArgumentException("All fields are required to create a query");
    }

    if (type != QueryType.INSERT && type != QueryType.UPDATE && type != QueryType.CREATE_TABLE && type != QueryType.DELETE) {
      throw new IllegalArgumentException("Invalid params for this query type: " + type);
    }

    this.type = type;
    this.tableName = tableName;
    this.values = values;

    // set not required fields to null
    this.condition = null;
    this.fields = null;
  }

  // for SELECT query
  public Query(QueryType type, String tableName, List<String> fields, Map<String, Object> condition) {
    if (type == null || tableName == null) {
      throw new IllegalArgumentException("All fields are required to create a query");
    }

    if (type != QueryType.SELECT) {
      throw new IllegalArgumentException("Invalid params for this query type: " + type);
    }

    this.type = type;
    this.tableName = tableName;
    this.fields = fields; // fields can be null to select all the fields
    this.condition = condition; // condition can be null to select all

    // set not required fields to null
    this.values = null;
  }
}