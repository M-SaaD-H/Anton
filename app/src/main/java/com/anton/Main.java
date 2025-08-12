package com.anton;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import com.anton.db.DBService;

public class Main {
  public static void main(String[] args) {
    DBService db = new DBService("storage");
    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

    while (true) {
      try {
        String query = br.readLine();
        String[] queryParts = query.split(" ");

        switch (queryParts[0]) {
          case "CREATE":
            System.out.println(query.substring(queryParts[0].length()) + " entry created");
            break;
          
          case "DELETE":
            System.out.println(query.substring(queryParts[0].length()) + " entry deleted");
            break;

          case "SELECT":
            System.out.println(query.substring(queryParts[0].length()) + " entry selected");
            break;
          
          default:
            System.out.println("Invalid query");
            br.close();
            break;
        }
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        try {
          br.close();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
  }
}