package com.anton.entites;

import java.time.Instant;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class User extends Entity {
  private String name;
  private String email;
  private String password;

  public User(String id, String name, String email, String password) {
    super(id, Instant.now().toEpochMilli(), Instant.now().toEpochMilli());
    this.name = name;
    this.email = email;
    this.password = password;
  }
}
