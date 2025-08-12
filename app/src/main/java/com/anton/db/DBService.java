package com.anton.db;

import java.io.File;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.anton.entites.Entity;

public class DBService {
	private final String storagePath;
	private final ObjectMapper objectMapper = new ObjectMapper();
	private List<Entity> entitiesList;

	public DBService(String storagePath) {
		this.storagePath = storagePath;
	}

	public void create(Entity entity) {
		entitiesList.add(entity);
	}

	public void delete(String id) {
		entitiesList.removeIf(e -> e.getId() == id);
	}

	public Entity findById(String id) {
		return entitiesList
			.stream()
			.filter(e -> e.getId().equals(id))
			.findFirst()
			.orElse(null);
	}

	public List<Entity> find() {
		return entitiesList;
	}

	public List<Entity> find(Map<String, Object> params) {
		return entitiesList
			.stream()
			.filter(e -> {
				for (Map.Entry<String, Object> entry : params.entrySet()) {
					String propName = entry.getKey();
					Object value = entry.getValue();

					if (value == null || (value instanceof String && ((String) value).length() <= 0)) return false;
					
					try {
						String methodName = "get" + propName.substring(0, 1).toUpperCase() + propName.substring(1);
						Method method = Entity.class.getMethod(methodName);

						Object actualValue = method.invoke(e);
						return actualValue != null && actualValue.equals(value);
					} catch (Exception ex) {
						System.out.println("Error while extracting method name in find method E: " + ex.getMessage());
						ex.printStackTrace();
						return false;
					}
				}

				return true;
			})
			.collect(Collectors.toList());
	}

	public void loadData() {
		try {
			File entitysDataFile = new File(this.storagePath + "/users.json");
			entitiesList = objectMapper.readValue(entitysDataFile, new TypeReference<List<Entity>>() {});
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void saveData() {
		try {
			File EntityDataFile = new File(this.storagePath + "/users.json");
			objectMapper.writeValue(EntityDataFile, entitiesList);
		} catch (Exception e) {
			System.out.println("Error while saving data E: " + e.getMessage());
			e.printStackTrace();
		}
	}
}
