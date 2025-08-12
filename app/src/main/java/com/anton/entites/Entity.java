package com.anton.entites;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@AllArgsConstructor
@Getter
@Setter
public class Entity {
  private String id;
  private long createdAt;
  private long updatedAt;
}
