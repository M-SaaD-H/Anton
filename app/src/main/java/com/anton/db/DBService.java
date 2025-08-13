package com.anton.db;

import java.io.File;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.anton.entites.User;

public class DBService {
	private final String storagePath;
	private final ObjectMapper objectMapper = new ObjectMapper();
	private List<User> usersList;

	public DBService(String storagePath) {
		this.storagePath = storagePath;
		this.usersList = loadData(this.storagePath + "/users.json", User.class);
	}

	public void create(User user) {
		usersList.add(user);
		saveData();
	}

	public void delete(String id) {
		usersList = usersList.stream().filter(u -> !id.equals(u.getId())).toList();
		saveData();
	}

	public User findById(String id) {
		return usersList
			.stream()
			.filter(e -> e.getId().equals(id))
			.findFirst()
			.orElse(null);
	}

	public List<User> find() {
		return usersList;
	}

	public List<User> find(Map<String, Object> params) {
		return usersList
			.stream()
			.filter(e -> {
				for (Map.Entry<String, Object> entry : params.entrySet()) {
					String propName = entry.getKey();
					Object value = entry.getValue();

					if (value == null || (value instanceof String && ((String) value).length() <= 0)) return false;
					
					try {
						String methodName = "get" + propName.substring(0, 1).toUpperCase() + propName.substring(1);
						Method method = User.class.getMethod(methodName);

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

	public <T> List<T> loadData(String filePath, Class<T> c) {
		try {
			File data = new File(filePath);
			CollectionType listType = objectMapper.getTypeFactory().constructCollectionType(List.class, c);
			return objectMapper.readValue(data, listType);
		} catch (Exception e) {
			System.out.println("Error while loading data E: " + e.getMessage());
			e.printStackTrace();
			return null;
		}
	}

	public void saveData() {
		try {
			File entitiesDataFile = new File(this.storagePath + "/users.json");
			objectMapper.writerWithDefaultPrettyPrinter().writeValue(entitiesDataFile, usersList);
		} catch (Exception e) {
			System.out.println("Error while saving data E: " + e.getMessage());
			e.printStackTrace();
		}
	}
}
