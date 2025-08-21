package com.anton.sql;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.anton.record.CatalogManager;
import com.anton.record.Column;
import com.anton.record.DataType;
import com.anton.record.Tuple;

public class QueryExecutor {
  private final QueryParser parser;
  private final CatalogManager db;

  public QueryExecutor() {
    this.parser = new QueryParser();
    CatalogManager db = null;
    try {
      db = new CatalogManager();
    } catch (Exception e) {
      System.out.println("Failed to initiate QueryExecutor. E: " + e.getMessage());
      e.printStackTrace();
    }
    this.db = db;
  }

  public List<Tuple> execute(String query) {
    Query q = parser.parse(query);

    return switch (q.getType()) {
      case CREATE_TABLE -> {
        executeCreateTable(q);
        yield null;
      }
      case INSERT -> {
        executeInsert(q);
        yield null;
      }
      case SELECT -> executeSelect(q);
      default -> {
        System.out.println(q);
        throw new IllegalArgumentException("Invalid query type");
      }
    };
  }

  public void executeCreateTable(Query q) {
    Map<String, Object> vals = q.getValues();
    if (vals == null) {
      throw new RuntimeException("Invalid value map recieved while creating a new table in executor");
    }

    List<Column> schema = new ArrayList<>();
    for (String val : vals.keySet()) {
      Column col = new Column(val, DataType.valueOf(vals.get(val).toString()));
      schema.add(col);
    }
    try {
      db.createTable(q.getTableName(), schema);
    } catch (Exception e) {
      System.out.println("Failed to create table: " + q.getTableName());
      e.printStackTrace();
    }
  }

  public void executeInsert(Query q) {
    Tuple tuple = new Tuple(q.getValues());
    try {
      db.insertTuple(q.getTableName(), tuple); // create this tuple
    } catch (Exception e) {
      System.out.println("Failed to insert data in " + q.getTableName());
      e.printStackTrace();
    }
  }

  public List<Tuple> executeSelect(Query q) {
    // TODO: Add filtering
    try {
      return db.selectTuples(q.getTableName());
    } catch (Exception e) {
      System.out.println("Failed to select tuples of table: " + q.getTableName() + ". E: " + e.getMessage());
      e.printStackTrace();
      return null;
    }
  }
}
