package com.anton;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

import com.anton.record.Tuple;
import com.anton.sql.BPlusTree;
import com.anton.sql.QueryExecutor;
import com.anton.storage.Slot;

public class Main {
  public static void main(String[] args) {
    System.out.println("Anton is running.");
    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

    BPlusTree<Integer> tree = new BPlusTree<>(4);

    for (int i = 1; i <= 20; i++) {
			tree.insert(i * 10, new Slot(i, i * 100));
		}
		
		for (int i = 1; i <= 20; i++) {
			tree.delete(i * 10);
		}

    System.out.println("size in main: " + tree.size());
    
    // tree.delete(70);

    System.out.println("isEmpty: " + tree.isEmpty());

    while (true) {
      try {
        String query = br.readLine();
        if (query == null || query.equals("EXIT")) return;
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
      if (result.size() <= 0) {
        System.out.println("No entries found.");
        return;
      }
      System.out.println("Result:\n");
      for (Tuple tuple : result) {
        for (String val : tuple.getValues().keySet()) {
          System.out.printf("%s: %s%n", val, tuple.getValue(val));
        }
        System.out.println();
      }
    }
  }
}