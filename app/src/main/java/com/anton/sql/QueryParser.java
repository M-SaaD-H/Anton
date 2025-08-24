package com.anton.sql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueryParser {
  // Parser
  public Query parse(String query) {
    query = query.trim();

    if (query.startsWith("CREATE TABLE")) {
      return parseCreateTable(query);
    } else if (query.startsWith("INSERT INTO")) {
      return parseInsert(query);
    } else if (query.startsWith("SELECT")) {
      return parseSelect(query);
    } else {
      throw new IllegalArgumentException("Unsupported query: " + query);
    }
  }

  // CREATE TABLE <TABLE_NAME> (<FIELDS_WITH_DATA_TYPES>)
  // e.g. CREATE TABLE users ('id' INT, 'name' STRING)
  private Query parseCreateTable(String query) {
    query = query.substring("CREATE TABLE".length()).trim();
    String tableName = query.substring(0, query.indexOf(" "));

    String valuesString = query.substring(query.indexOf("(") + 1, query.indexOf(")"));
    String[] valuesPart = valuesString.split(",");
    Map<String, Object> values = new HashMap<>();

    // build value map
    for (String val : valuesPart) {
      String[] splitVal = val.trim().split(" ");
      String fieldName = splitVal[0].replace("'", "").trim(); // strip quotes
      String fieldValue = splitVal[1].replace("'", "").trim();

      values.put(fieldName, fieldValue);
    }

    return new Query(QueryType.CREATE_TABLE, tableName, values); // values = schema
  }

  // INSERT INTO <TABLE_NAME> VALUES (<FIELDS_WITH_VALUES>)
  // e.g. INSERT INTO users VALUES ('id' 1, 'name', 'Anton')
  private Query parseInsert(String query) {
    query = query.substring("INSERT INTO".length()).trim();
    String tableName = query.substring(0, query.indexOf(" ")).trim();

    String valuesString = query.substring(query.indexOf("(") + 1, query.indexOf(")"));
    String[] valuesPart = valuesString.split(",");
    Map<String, Object> values = new HashMap<>();

    // build value map
    for (String val : valuesPart) {
      String[] splitVal = val.trim().split(" ");
      String fieldName = splitVal[0].replace("'", "").trim(); // strip quotes
      String fieldValue = splitVal[1].replace("'", "").trim();

      values.put(fieldName, fieldValue);
    }

    return new Query(QueryType.INSERT, tableName, values);
  }

  // SELECT <PARAMS OR *> FROM <TABLE_NAME> WHERE <CONDITION>
  // e.g. SELECT id, name FROM users WHERE id=1
  private Query parseSelect(String query) {
    query = query.substring("SELECT".length()).trim();
    int fromIdx = query.indexOf("FROM");

    String fields = query.substring(0, fromIdx).trim();
    List<String> fieldsToSelect = null;
    if (!fields.equals("*")) {
      fieldsToSelect = new ArrayList<>();
      String[] fieldsString = fields.split(",");
      for (String f : fieldsString) {
        fieldsToSelect.add(f.toLowerCase().trim());
      }
    }

    String tableName = query.substring(fromIdx + "FROM".length()).trim();

    // values for the conditional selection
    Map<String, Object> conditions = null;
    if (query.contains("WHERE")) {
      tableName = tableName.substring(0, tableName.indexOf("WHERE")).trim();
      String conditionString = query.substring(query.indexOf("WHERE") + "WHERE".length()).trim();
      String[] conditionParts = conditionString.split("&");

      // build condition map
      conditions = new HashMap<>();
      for (String con : conditionParts) {
        String[] splitVal = con.trim().split("=");
        String fieldName = splitVal[0].trim(); // strip quotes
        String fieldValue = splitVal[1].trim();

        conditions.put(fieldName, fieldValue);
      }
    }

    return new Query(QueryType.SELECT, tableName, fieldsToSelect, conditions);
  }
}
