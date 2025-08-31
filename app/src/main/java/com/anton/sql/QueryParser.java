package com.anton.sql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueryParser {
  // Parser
  public Query parse(String query) throws IllegalArgumentException {
    query = query.trim();

    if (query.startsWith("CREATE TABLE")) {
      return parseCreateTable(query);
    } else if (query.startsWith("INSERT INTO")) {
      return parseInsert(query);
    } else if (query.startsWith("SELECT")) {
      return parseSelect(query);
    } else if (query.startsWith("DROP")) {
      return parseDrop(query);
    } else if (query.startsWith("DELETE")) {
      return parseDelete(query);
    } else {
      throw new IllegalArgumentException("Unsupported query: " + query);
    }
  }

  // CREATE TABLE <TABLE_NAME> (<FIELDS_WITH_DATA_TYPES>)
  // e.g. CREATE TABLE users ('id' INT, 'name' STRING)
  private Query parseCreateTable(String query) throws IllegalArgumentException {
    // Remove "CREATE TABLE" and trim
    if (!query.toUpperCase().startsWith("CREATE TABLE")) {
      throw new IllegalArgumentException("Query must start with 'CREATE TABLE'");
    }
    query = query.substring("CREATE TABLE".length()).trim();

    // Find table name (should be before first space or first '(')
    int openParenIdx = query.indexOf('(');
    if (openParenIdx == -1) {
      throw new IllegalArgumentException("Missing '(' after table name in CREATE TABLE statement.");
    }
    String tableName = query.substring(0, openParenIdx).trim();
    if (tableName.isEmpty()) {
      throw new IllegalArgumentException("Table name is missing in CREATE TABLE statement.");
    }

    // Extract values string inside parentheses
    int closeParenIdx = query.indexOf(')', openParenIdx);
    if (closeParenIdx == -1) {
      throw new IllegalArgumentException("Missing closing ')' in CREATE TABLE statement.");
    }
    String valuesString = query.substring(openParenIdx + 1, closeParenIdx).trim();
    if (valuesString.isEmpty()) {
      throw new IllegalArgumentException("No columns specified in CREATE TABLE statement.");
    }

    String[] valuesPart = valuesString.split(",");
    Map<String, Object> values = new HashMap<>();

    // build value map
    for (String val : valuesPart) {
      val = val.trim();
      if (val.isEmpty())
        continue;
      String[] splitVal = val.split("\\s+");
      if (splitVal.length != 2) {
        throw new IllegalArgumentException("Invalid column definition: '" + val + "'. Expected format: 'name TYPE'");
      }

      String fieldName = splitVal[0].replace("'", "").replace("\"", "").trim(); // strip quotes
      String fieldValue = splitVal[1].replace("'", "").replace("\"", "").trim();
      if (fieldName.isEmpty() || fieldValue.isEmpty()) {
        throw new IllegalArgumentException("Column name or type cannot be empty in: '" + val + "'");
      }
      if (values.containsKey(fieldName)) {
        throw new IllegalArgumentException("Duplicate column name: '" + fieldName + "'");
      }

      values.put(fieldName, fieldValue);
    }

    if (values.isEmpty()) {
      throw new IllegalArgumentException("No valid columns found in CREATE TABLE statement.");
    }

    return new Query(QueryType.CREATE_TABLE, tableName, values); // values = schema
  }

  // INSERT INTO <TABLE_NAME> VALUES (<FIELDS_WITH_VALUES>)
  // e.g. INSERT INTO users VALUES ('id' 1, 'name', 'Anton')
  private Query parseInsert(String query) throws IllegalArgumentException {
    // Remove "INSERT INTO" and trim
    if (!query.toUpperCase().startsWith("INSERT INTO")) {
      throw new IllegalArgumentException("Query must start with 'INSERT INTO'");
    }
    query = query.substring("INSERT INTO".length()).trim();
    if (!query.contains("VALUES")) {
      throw new IllegalArgumentException("Missing VALUES clause in the INSERT INTO.");
    }
    String tableName = query.substring(0, query.indexOf("VALUES")).trim();

    if (tableName.isEmpty()) {
      throw new IllegalArgumentException("Table name is missing in INSERT INTO statement.");
    }

    int openParenIdx = query.indexOf('(');
    if (openParenIdx == -1) {
      throw new IllegalArgumentException("Missing '(' after table name in INSERT INTO statement.");
    }
    int closeParenIdx = query.indexOf(')', openParenIdx);
    if (closeParenIdx == -1) {
      throw new IllegalArgumentException("Missing closing ')' in INSERT INTO statement.");
    }

    String valuesString = query.substring(openParenIdx + 1, closeParenIdx).trim();
    if (valuesString.isEmpty()) {
      throw new IllegalArgumentException("No values specified in INSERT INTO statement.");
    }

    String[] valuesPart = valuesString.split(",");
    Map<String, Object> values = new HashMap<>();

    // build value map
    for (String val : valuesPart) {
      val = val.trim();
      if (val.isEmpty())
        continue;
      String[] splitVal = val.split("\\s+");
      if (splitVal.length != 2) {
        throw new IllegalArgumentException("Invalid field-value pair: '" + val + "'. Expected format: 'name value'");
      }

      String fieldName = splitVal[0].replace("'", "").replace("\"", "").trim(); // strip quotes
      String fieldValue = splitVal[1].replace("'", "").replace("\"", "").trim();
      if (fieldName.isEmpty() || fieldValue.isEmpty()) {
        throw new IllegalArgumentException("Field name or value cannot be empty in: '" + val + "'");
      }
      if (values.containsKey(fieldName)) {
        throw new IllegalArgumentException("Duplicate field name: '" + fieldName + "'");
      }

      values.put(fieldName, fieldValue);
    }

    if (values.isEmpty()) {
      throw new IllegalArgumentException("No valid field-value pairs found in INSERT INTO statement.");
    }

    return new Query(QueryType.INSERT, tableName, values);
  }

  // SELECT <PARAMS OR *> FROM <TABLE_NAME> WHERE <CONDITION>
  // e.g. SELECT id, name FROM users WHERE id=1
  private Query parseSelect(String query) throws IllegalArgumentException {
    // Basic validation
    if (!query.toUpperCase().startsWith("SELECT")) {
      throw new IllegalArgumentException("Query must start with SELECT");
    }
    query = query.substring("SELECT".length()).trim();

    int fromIdx = query.toUpperCase().indexOf("FROM");
    if (fromIdx == -1) {
      throw new IllegalArgumentException("Missing FROM in SELECT statement.");
    }

    String fields = query.substring(0, fromIdx).trim();
    if (fields.isEmpty()) {
      throw new IllegalArgumentException("No fields specified in SELECT statement.");
    }

    List<String> fieldsToSelect = null;
    if (!fields.equals("*")) {
      fieldsToSelect = new ArrayList<>();
      String[] fieldsString = fields.split(",");
      for (String f : fieldsString) {
        f = f.trim();
        if (f.isEmpty()) {
          throw new IllegalArgumentException("Empty field name in SELECT statement.");
        }
        fieldsToSelect.add(f.toLowerCase());
      }
    }

    String tableName = query.substring(fromIdx + "FROM".length()).trim();

    if (query.contains("WHERE")) {
      tableName = tableName.substring(0, tableName.indexOf("WHERE")).trim();
    }

    if (tableName.isEmpty()) {
      throw new IllegalArgumentException("Missing table name in SELECT statement.");
    }

    // values for conditional selection
    Map<String, Object> conditions = null;
    if (query.contains("WHERE")) {
      int whereIdx = query.toUpperCase().indexOf("WHERE");
      String conditionString = query.substring(whereIdx + "WHERE".length()).trim();
      if (conditionString.isEmpty()) {
        throw new IllegalArgumentException("WHERE clause is empty in SELECT statement.");
      }
      String[] conditionParts = conditionString.split("&");

      // build condition map
      conditions = new HashMap<>();
      for (String con : conditionParts) {
        String[] splitVal = con.trim().split("=");
        if (splitVal.length != 2) {
          throw new IllegalArgumentException("Invalid condition: '" + con + "'. Expected format: field=value");
        }
        String fieldName = splitVal[0].trim();
        String fieldValue = splitVal[1].trim();
        if (fieldName.isEmpty() || fieldValue.isEmpty()) {
          throw new IllegalArgumentException("Condition field or value cannot be empty in: '" + con + "'");
        }
        if (conditions.containsKey(fieldName)) {
          throw new IllegalArgumentException("Duplicate condition field: '" + fieldName + "'");
        }
        conditions.put(fieldName, fieldValue);
      }

      if (conditions.isEmpty()) {
        throw new IllegalArgumentException("Conditions are empty after the WHERE clause in the SELECT statement.");
      }
    }

    return new Query(QueryType.SELECT, tableName, fieldsToSelect, conditions);
  }

  // DROP TABLE <TABLE_NAME>
  // e.g. DROP TABLE users
  private Query parseDrop(String query) throws IllegalArgumentException {
    if (!query.toUpperCase().startsWith("DROP")) {
      throw new IllegalArgumentException("Query must start with DROP");
    }
    query = query.substring("DROP".length()).trim();
    String tableName = query.substring("TABLE".length()).trim();
    if (tableName.isEmpty()) {
      throw new IllegalArgumentException("Missing table name in DELETE statement.");
    }

    return new Query(QueryType.DELETE, tableName, new HashMap<>());
  }

  // DELETE FROM <TABLE_NAME> WHERE <CONDITION>
  // e.g. DELETE FROM users WHERE id=123
  private Query parseDelete(String query) throws IllegalArgumentException {
    String tableName = query.substring("FROM".length(), query.toUpperCase().indexOf("WHERE")).trim();
    if (tableName.isEmpty()) {
      throw new IllegalArgumentException("Missing table name in DELETE statement.");
    }

    String conditionString = query.substring(query.toUpperCase().indexOf("WHERE") + "WHERE".length()).trim();
    if (conditionString.isEmpty()) {
      throw new IllegalArgumentException("WHERE clause is empty in DELETE statement.");
    }
    String[] conditionParts = conditionString.split("&");

    // build condition map
    Map<String, Object> conditions = new HashMap<>();
    for (String con : conditionParts) {
      String[] splitVal = con.trim().split("=");
      if (splitVal.length != 2) {
        throw new IllegalArgumentException("Invalid condition: '" + con + "'. Expected format: field=value");
      }
      String fieldName = splitVal[0].trim();
      String fieldValue = splitVal[1].trim();
      if (fieldName.isEmpty() || fieldValue.isEmpty()) {
        throw new IllegalArgumentException("Condition field or value cannot be empty in: '" + con + "'");
      }
      if (conditions.containsKey(fieldName)) {
        throw new IllegalArgumentException("Duplicate condition field: '" + fieldName + "'");
      }
      conditions.put(fieldName, fieldValue);
    }

    if (conditions.isEmpty()) {
      throw new IllegalArgumentException("Conditions are empty after the WHERE clause in the SELECT statement.");
    }

    return new Query(null, tableName, null, conditions);
  }
}
