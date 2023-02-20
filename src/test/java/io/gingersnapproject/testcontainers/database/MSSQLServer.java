package io.gingersnapproject.testcontainers.database;

import java.util.Map;

import io.gingersnapproject.testcontainers.DatabaseProvider;

import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.utility.DockerImageName;

public class MSSQLServer implements DatabaseProvider {
   private static final String IMAGE_NAME = "mcr.microsoft.com/mssql/server:2019-latest";
   private static final String STRONG_PASSWORD = "Password!42";

   @Override
   public JdbcDatabaseContainer<?> createDatabase() {
      MSSQLServerContainer<?> database = new MSSQLServerContainer<>(DockerImageName.parse(IMAGE_NAME))
            .acceptLicense();
      database
            .withPassword(STRONG_PASSWORD)
            .withEnv("MSSQL_AGENT_ENABLED", "true")
            .withExposedPorts(MSSQLServerContainer.MS_SQL_SERVER_PORT)
            .withInitScript("mssql/setup.sql");

      database.start();
      return database.withUrlParam("Database", "debezium");
   }

   @Override
   public Map<String, String> properties() {
      return Map.of(
            "gingersnap.database.type", "SQLSERVER",
            "gingersnap.database.database", "debezium",
            "gingersnap.database.username", "gingersnap_login",
            "gingersnap.database.password", STRONG_PASSWORD
      );
   }
}
