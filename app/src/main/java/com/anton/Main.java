package com.anton;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

import com.anton.record.Tuple;
import com.anton.sql.QueryExecutor;

public class Main {
  public static void main(String[] args) {
    System.out.println("Anton is running.");
    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

    while (true) {
      try {
        String query = br.readLine();
        queryProcessor(query);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  public static void queryProcessor(String query) throws IOException {
    QueryExecutor executor = new QueryExecutor();
    List<Tuple> result = executor.execute(query);

    if (result != null) {
      System.out.println("Result-");
      for (Tuple tuple : result) {
        System.out.printf("id: %S%n", tuple.getValue("id"));
        System.out.printf("name: %s%n", tuple.getValue("name"));
      }
    }
  }
}