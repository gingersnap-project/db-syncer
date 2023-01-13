package io.gingersnapproject.testcontainers;

import java.time.Duration;
import java.util.Map;

import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;

public class MySQLResources extends BaseGingersnapResourceLifecycleManager {
   private static final String IMAGE = "mysql:8.0.31";

   @Override
   protected void enrichProperties(Map<String, String> properties) {
      properties.put("gingersnap.database.type", "MYSQL");
   }

   @Override
   protected JdbcDatabaseContainer<?> createDatabase() {
      return new MySQLContainer<>(IMAGE)
            .withUsername("gingersnap_user")
            .withPassword("password")
            .withExposedPorts(MySQLContainer.MYSQL_PORT)
            .withStartupTimeout(Duration.ofSeconds(30))
            .waitingFor(Wait.forLogMessage(".*mysqld: ready for connections.*", 2))
            .withCopyFileToContainer(MountableFile.forClasspathResource("mysql/setup.sql"), "/docker-entrypoint-initdb.d/setup.sql");
   }
}
