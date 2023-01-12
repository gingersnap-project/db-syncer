package io.gingersnapproject.testcontainers.database;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;

import io.gingersnapproject.testcontainers.DatabaseProvider;

import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;

public class MySQL implements DatabaseProvider {
   private static final String IMAGE = "mysql:8.0.31";
   private static final String CONTAINER_DATA_DIR = "/var/lib/mysql";
   private static final String HOST_TMP_DIR = Path.of(System.getProperty("java.io.tmpdir"), "mysql_db_syncer").toString();

   @Override
   public Map<String, String> properties() {
      return Map.of(
            "gingersnap.database.type", "MYSQL"
      );
   }

   @Override
   public JdbcDatabaseContainer<?> createDatabase() {
      return new MySQLContainer<>(IMAGE)
            .withUsername("gingersnap_user")
            .withPassword("password")
            .withDatabaseName("debezium")
            .withExposedPorts(MySQLContainer.MYSQL_PORT)
            .withStartupTimeout(Duration.ofSeconds(30))
            .waitingFor(Wait.forLogMessage(".*mysqld: ready for connections.*", 2))
            .withFileSystemBind(HOST_TMP_DIR, CONTAINER_DATA_DIR, BindMode.READ_WRITE)
            .withCopyFileToContainer(MountableFile.forClasspathResource("mysql/setup.sql"), "/docker-entrypoint-initdb.d/setup.sql");
   }
}
