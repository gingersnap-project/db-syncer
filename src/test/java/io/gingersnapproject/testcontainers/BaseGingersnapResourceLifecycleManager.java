package io.gingersnapproject.testcontainers;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import io.gingersnapproject.testcontainers.annotation.KeyValue;
import io.gingersnapproject.testcontainers.annotation.WithDatabase;

import io.quarkus.test.common.QuarkusTestResourceConfigurableLifecycleManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.JdbcDatabaseContainer;

public class BaseGingersnapResourceLifecycleManager implements
      QuarkusTestResourceConfigurableLifecycleManager<WithDatabase> {

   private static final Logger log = LoggerFactory.getLogger(BaseGingersnapResourceLifecycleManager.class);
   private static final String LABEL = "io.gingersnapproject.test";
   protected static final AtomicInteger users = new AtomicInteger();
   private CacheManagerContainer cacheManager;
   private JdbcDatabaseContainer<?> database;
   private final Map<String, String> runtimeProperties = new HashMap<>();
   private DatabaseProvider delegate;
   private String ruleName;

   @Override
   public final void init(WithDatabase annotation) {
      var clazz = annotation.value();

      try {
         this.delegate = clazz.getConstructor().newInstance();
         ruleName = annotation.rule();
         runtimeProperties.putAll(convert(annotation.properties()));
      } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
         throw new RuntimeException(String.format("Failed instantiating: %s", clazz.getSimpleName()));
      }
   }

   @Override
   public Map<String, String> start() {
      String label = String.format("container-%d", users.incrementAndGet());
      log.info("Starting container with label {}", label);
      database = delegate.createDatabase(String.valueOf(users.get()));

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

      if (!ruleName.isEmpty()) {
         properties.putAll(Map.of(
               "gingersnap.rule.%s.connector.schema".formatted(ruleName), "debezium",
               "gingersnap.rule.%s.connector.table".formatted(ruleName), "customer",
               "gingersnap.rule.%s.key-columns".formatted(ruleName), "fullname"
         ));
      }

      properties.putAll(delegate.properties());
      properties.putAll(runtimeProperties);
      return properties;
   }

   @Override
   public void stop() {
      if (cacheManager != null) cacheManager.stop();
      if (database != null) database.stop();
   }

   private Map<String, String> convert(KeyValue[] values) {
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
