package com.anton.record;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Tuple {
  private final Map<String, Object> values;

  public Tuple(Map<String, Object> values) {
    this.values = values;
  }

  // validate the data according to the schema return its byte form
  public byte[] toBytes(List<Column> schema) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (DataOutputStream dos = new DataOutputStream(baos)) {
      for (Column col : schema) {
        Object val = values.get(col.getName());
        switch (col.getType()) {
          case INT -> dos.writeInt((Integer) val);

          case STRING -> {
            byte[] strBytes = ((String) val).getBytes(StandardCharsets.UTF_8);
            dos.writeInt(strBytes.length); // stores length first
            dos.write(strBytes); // store the data
          }

          // Add more data types (if needed)

          default -> throw new IllegalArgumentException("Unknown column type: " + col.getType());
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return baos.toByteArray();
  }

  public static Tuple fromBytes(byte[] data, List<Column> schema) {
    Map<String, Object> vals = new HashMap<>();

    ByteArrayInputStream bais = new ByteArrayInputStream(data);
    try (DataInputStream dis = new DataInputStream(bais)) {
      for (Column col : schema) {
        // Object val = vals.get(col.getName());
        switch (col.getType()) {
          case INT -> {
            int val = dis.readInt();
            vals.put(col.getName(), val);
          }

          case STRING -> {
            int length = dis.readInt();
            byte[] strBytes = new byte[length];
            dis.readFully(strBytes);
            vals.put(col.getName(), new String(strBytes, StandardCharsets.UTF_8));
          }
        
          default -> throw new IllegalArgumentException("Unknown column type: " + col.getType());
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return new Tuple(vals);
  }
}
