package io.gingersnapproject.testcontainers;

import java.time.Duration;
import java.util.Map;

import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;

public class PostgresResources extends BaseGingersnapResourceLifecycleManager {
   private static final String IMAGE = "postgres:latest";

   @Override
   protected void enrichProperties(Map<String, String> properties) {
      properties.put("gingersnap.database.type", "POSTGRESQL");
      properties.put("gingersnap.database.database", "debeziumdb");
   }

   @Override
   protected JdbcDatabaseContainer<?> createDatabase() {
      JdbcDatabaseContainer<?> container = new PostgreSQLContainer(IMAGE)
            .withUsername("gingersnap_user")
            .withPassword("password")
            .withDatabaseName("debeziumdb");
      container.withExposedPorts(5432)
            .waitingFor(Wait.forLogMessage(".*database system is ready to accept connections.*", 2))
            .withStartupTimeout(Duration.ofSeconds(30))
            .withCopyFileToContainer(MountableFile.forClasspathResource("postgres/setup.sql"), "/docker-entrypoint-initdb.d/setup.sql")
            .withCommand("postgres", "-c", "wal_level=logical");
      return container;
   }
}
