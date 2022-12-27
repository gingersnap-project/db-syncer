package io.gingersnapproject.testcontainers;

import java.util.Map;

import org.testcontainers.containers.JdbcDatabaseContainer;

public interface DatabaseProvider {

   JdbcDatabaseContainer<?> createDatabase();

   Map<String, String> properties();
}
