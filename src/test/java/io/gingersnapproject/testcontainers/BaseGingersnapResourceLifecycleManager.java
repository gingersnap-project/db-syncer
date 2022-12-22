package io.gingersnapproject.testcontainers;

import java.lang.annotation.Annotation;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import io.gingersnapproject.testcontainers.annotation.KeyValue;

import io.quarkus.test.common.QuarkusTestResourceConfigurableLifecycleManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.JdbcDatabaseContainer;

public abstract class BaseGingersnapResourceLifecycleManager<T extends Annotation>
      implements QuarkusTestResourceConfigurableLifecycleManager<T> {

   private static final Logger log = LoggerFactory.getLogger(BaseGingersnapResourceLifecycleManager.class);

   public static final String RULE_NAME = "us-east";
   public static final String REMOVE_DEFAULT_RULE = "test.default.rule.remove";
   private static final String LABEL = "io.gingersnapproject.test";
   protected static final AtomicInteger users = new AtomicInteger();
   private CacheManagerContainer cacheManager;
   private JdbcDatabaseContainer<?> database;
   private final Map<String, String> runtimeProperties = new HashMap<>();

   @Override
   public final void init(T annotation) {
      runtimeProperties.putAll(convert(annotation));
   }

   @Override
   public Map<String, String> start() {
      String label = String.format("container-%d", users.get());
      log.info("Starting container with label {}", label);
      database = createDatabase();

      assert database != null : "Database can not be null";

      database.withLabel(LABEL, label);

      if (!database.isRunning()) database.start();

      Testcontainers.exposeHostPorts(database.getFirstMappedPort());
      cacheManager = new CacheManagerContainer()
            .withDatabaseUrl(String.format("%s://host.testcontainers.internal:%s/%s", databaseKind(database.getJdbcUrl()), database.getFirstMappedPort(), database.getDatabaseName()))
            .withLabel(LABEL, label);
      cacheManager.start();

      Map<String, String> properties = new HashMap<>(Map.of(
            "gingersnap.cache.uri", cacheManager.hotrodUri(),
            "gingersnap.database.host", database.getHost(),
            "gingersnap.database.port", Integer.toString(database.getFirstMappedPort()),
            "gingersnap.database.user", database.getUsername(),
            "gingersnap.database.password", database.getPassword()
      ));

      if (!runtimeProperties.containsKey(REMOVE_DEFAULT_RULE)) {
         properties.putAll(Map.of(
               "gingersnap.rule.%s.connector.schema".formatted(RULE_NAME), "debezium",
               "gingersnap.rule.%s.connector.table".formatted(RULE_NAME), "customer",
               "gingersnap.rule.%s.key-columns".formatted(RULE_NAME), "fullname"
         ));
      }

      enrichProperties(properties);
      properties.putAll(runtimeProperties);
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

   protected abstract Map<String, String> convert(T annotation);

   protected final Map<String, String> convert(KeyValue[] values) {
      Map<String, String> properties = new HashMap<>();
      for (KeyValue kv : values) {
         properties.put(kv.key(), kv.value());
      }
      return properties;
   }

   public static String databaseKind(String url) {
      String replace = url.replace("jdbc:", "");
      String[] values = replace.split(":");
      return values[0];
   }
}
