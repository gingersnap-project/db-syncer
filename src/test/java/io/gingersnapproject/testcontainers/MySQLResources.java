package io.gingersnapproject.testcontainers;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;

import io.gingersnapproject.testcontainers.BaseGingersnapResourceLifecycleManager;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;

public class MySQLResources extends BaseGingersnapResourceLifecycleManager {
   private static final String IMAGE = "mysql:8.0.31";
   private static final String HOST_TMP = Path.of(System.getProperty("java.io.tmpdir"), "mysql").toString();
   private static final String CONTAINER_DATA_DIR = "/var/lib/mysql";

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
            .withFileSystemBind(HOST_TMP, CONTAINER_DATA_DIR, BindMode.READ_WRITE)
            .withCopyFileToContainer(MountableFile.forClasspathResource("mysql/setup.sql"), "/docker-entrypoint-initdb.d/setup.sql");
   }

   @Override
   public void stop() {
      super.stop();

      System.out.println("DELETING");
      try {
         Files.delete(Path.of(HOST_TMP));
      } catch (IOException ignore) { }
   }
}
