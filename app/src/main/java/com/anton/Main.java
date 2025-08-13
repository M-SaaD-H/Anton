package com.anton;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;

import com.anton.db.DBService;
import com.anton.entites.User;

public class Main {
  public static void main(String[] args) {
    DBService db = new DBService("storage");
    System.out.println("Anton is running.");
    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

    while (true) {
      try {
        String query = br.readLine();
        queryProcessor(query, br, db);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  public static void queryProcessor(String query, BufferedReader br, DBService db) throws IOException {
    String[] queryParts = query != null ? query.split(" ") : new String[]{""};

    switch (queryParts[0]) {
      case "CREATE":
        System.out.println("Enter id:");
        String id = br.readLine();
        System.out.println("Enter Name:");
        String name = br.readLine();
        System.out.println("Enter email:");
        String email = br.readLine();
        System.out.println("Enter password:");
        String password = br.readLine();

        db.create(new User(id, name, email, password));

        break;
      case "DELETE":
        String idToDelete = queryParts[queryParts.length - 1].split("=")[1];
        db.delete(idToDelete);
        
        break;
      case "SELECT":
        if (queryParts[queryParts.length - 2].equals("WHERE")) {
          String propString = queryParts[queryParts.length - 1];
          String[] propStringParts = propString.split("=");

          if (propStringParts[0].equals("id")) {
            User user = db.findById(propStringParts[1]);
            if (user == null) {
              System.out.println("User not found");
            } else {
              System.out.println(user.toString());
            }
            break;
          }

          List<User> users = db.find(Map.of(propStringParts[0], propStringParts[1]));
          if (users == null || users.size() <= 0) {
            System.out.println("Users not found");
          } else {
            for (User u : users) {
              System.out.println(u.toString());
            }
          }
          break;
        }

        List<User> allUsers = db.find();
        for (User u : allUsers) {
          System.out.println(u.toString());
        }
        break;
      default:
        System.out.println("Invalid query");
        break;
    }
  }
}