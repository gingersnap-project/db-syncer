package io.gingersnapproject.testcontainers;

import java.util.HashMap;
import java.util.Map;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.JdbcDatabaseContainer;

public abstract class BaseGingersnapResourceLifecycleManager implements QuarkusTestResourceLifecycleManager {

   private CacheManagerContainer cacheManager;

   private JdbcDatabaseContainer<?> database;

   @Override
   public Map<String, String> start() {
      database = createDatabase();

      assert database != null : "Database can not be null";

      if (!database.isRunning()) database.start();

      Testcontainers.exposeHostPorts(database.getFirstMappedPort());
      cacheManager = new CacheManagerContainer()
            .withDatabaseUrl(String.format("%s://host.testcontainers.internal:%s/%s", databaseKind(database.getJdbcUrl()), database.getFirstMappedPort(), database.getDatabaseName()));
      cacheManager.start();

      Map<String, String> properties = new HashMap<>(Map.of(
            "gingersnap.cache.uri", cacheManager.hotrodUri(),
            "gingersnap.database.host", database.getHost(),
            "gingersnap.database.port", Integer.toString(database.getFirstMappedPort()),
            "gingersnap.database.user", database.getUsername(),
            "gingersnap.database.password", database.getPassword(),
            "gingersnap.rule.us-east.connector.schema", "debezium",
            "gingersnap.rule.us-east.connector.table", "customer",
            "gingersnap.rule.us-east.key-columns", "fullname"
      ));
      enrichProperties(properties);
      return properties;
   }

   @Override
   public void stop() {
      if (cacheManager != null) cacheManager.stop();
      if (database != null) database.stop();
   }

   protected void enrichProperties(Map<String, String> properties) {
      // No-op
   }

   protected abstract JdbcDatabaseContainer<?> createDatabase();

   public static String databaseKind(String url) {
      String replace = url.replace("jdbc:", "");
      String[] values = replace.split(":");
      return values[0];
   }
}
