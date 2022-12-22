package io.gingersnapproject.testcontainers;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;

import io.gingersnapproject.testcontainers.annotation.WithMySQL;

import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;

public class MySQLResources extends BaseGingersnapResourceLifecycleManager<WithMySQL> {
   private static final String IMAGE = "mysql:8.0.31";
   private static final String CONTAINER_DATA_DIR = "/var/lib/mysql";

   @Override
   protected void enrichProperties(Map<String, String> properties) {
      properties.put("gingersnap.database.type", "MYSQL");
   }

   @Override
   protected JdbcDatabaseContainer<?> createDatabase() {
      String tmp = Path.of(System.getProperty("java.io.tmpdir"), "mysql_db_syncer", String.valueOf(users.get())).toString();
      return new MySQLContainer<>(IMAGE)
            .withUsername("gingersnap_user")
            .withPassword("password")
            .withExposedPorts(MySQLContainer.MYSQL_PORT)
            .withStartupTimeout(Duration.ofSeconds(30))
            .waitingFor(Wait.forLogMessage(".*mysqld: ready for connections.*", 2))
            .withFileSystemBind(tmp, CONTAINER_DATA_DIR, BindMode.READ_WRITE)
            .withCopyFileToContainer(MountableFile.forClasspathResource("mysql/setup.sql"), "/docker-entrypoint-initdb.d/setup.sql");
   }

   @Override
   protected Map<String, String> convert(WithMySQL annotation) {
      return convert(annotation.properties());
   }
}
