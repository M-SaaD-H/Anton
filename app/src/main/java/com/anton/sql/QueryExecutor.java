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

  public QueryExecutor(CatalogManager db) {
    this.parser = new QueryParser();
    this.db = db;
  }

  public List<Tuple> execute(String query) {
    Query q = null;
    try {
      q = parser.parse(query);
    } catch (Exception e) {
      System.out.println("Error while parsing the query. E: " + e.getMessage());
      return null; // handle parse errors
    }

    if (q == null) {
      return null;
    }

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
      case DELETE -> {
        executeDelete(q);
        yield null;
      }
      case DROP_TABLE -> {
        executeDropTable(q);
        yield null;
      }
      default -> {
        System.out.println(q);
        throw new IllegalArgumentException("Invalid query type");
      }
    };
  }

  public void executeCreateTable(Query query) {
    CreateTableQuery q = (CreateTableQuery) query;
    Map<String, String> vals = q.getColumns();
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
      System.out.println("Failed to create table: " + q.getTableName() + ". E: " + e.getMessage());
      e.printStackTrace();
    }
  }

  public void executeInsert(Query query) {
    InsertQuery q = (InsertQuery) query;
    Tuple tuple = new Tuple(q.getValues());
    try {
      db.insertTuple(q.getTableName(), tuple);
    } catch (Exception e) {
      System.out.println("Failed to insert data in " + q.getTableName() + ". E: " + e.getMessage());
      e.printStackTrace();
    }
  }

  public List<Tuple> executeSelect(Query query) {
    SelectQuery q = (SelectQuery) query;
    try {
      return db.selectTuples(q.getTableName(), q.getConditions(), q.getFields());
    } catch (Exception e) {
      System.out.println("Failed to select tuples of table: " + q.getTableName() + ". E: " + e.getMessage());
      e.printStackTrace();
      return null;
    }
  }

  public void executeDropTable(Query query) {
    DropTableQuery q = (DropTableQuery) query;
    try {
      db.dropTable(q.getTableName());
    } catch (Exception e) {
      System.out.println("Error while dropping the table: " + q.getTableName() + ". E: " + e.getMessage());
      e.printStackTrace();
    }
  }

  public void executeDelete(Query query) {
    DeleteQuery q = (DeleteQuery) query;
    try {
      db.delete(q.getTableName(), q.getConditions());
    } catch (Exception e) {
      System.out.println("Error while deleting table: " + q.getTableName() + ". E: " + e.getMessage());
      e.printStackTrace();
    }
  }
}
