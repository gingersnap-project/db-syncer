package io.gingersnapproject.testcontainers;

import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.sql.Driver;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import javax.enterprise.event.Observes;

import io.gingersnapproject.cdc.cache.CacheIdentifier;
import io.gingersnapproject.cdc.event.Events;
import io.gingersnapproject.testcontainers.annotation.ContainerInject;
import io.gingersnapproject.testcontainers.annotation.KeyValue;
import io.gingersnapproject.testcontainers.annotation.WithDatabase;
import io.gingersnapproject.testcontainers.hotrod.CacheManagerContainer;
import io.gingersnapproject.testcontainers.hotrod.HotRodContainer;
import io.gingersnapproject.testcontainers.hotrod.InfinispanContainer;

import io.quarkus.test.common.QuarkusTestResourceConfigurableLifecycleManager;
import io.quarkus.test.junit.callback.QuarkusTestBeforeClassCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.output.OutputFrame;

public class BaseGingersnapResourceLifecycleManager implements
      QuarkusTestResourceConfigurableLifecycleManager<WithDatabase>, QuarkusTestBeforeClassCallback {
   private static final Logger log = LoggerFactory.getLogger(BaseGingersnapResourceLifecycleManager.class);
   private static final Pattern RULE_NAME_PATTERN = Pattern.compile("^[a-z\\d]+$");
   private static final Map<CacheIdentifier, CountDownLatch> AUTOMATIC_STARTED_ENGINES = new ConcurrentHashMap<>();
   private HotRodContainer<?> cacheManager;
   private JdbcDatabaseContainer<?> database;
   private final Map<String, String> runtimeProperties = new HashMap<>();
   private DatabaseProvider delegate;
   private String[] rules;

   @Override
   public final void init(WithDatabase annotation) {
      var clazz = annotation.value();

      try {
         if (clazz.isInterface()) clazz = Profiles.databaseProviderClass();
         this.delegate = clazz.getConstructor().newInstance();
         rules = annotation.rules();
         for (String rule : rules) assertCompatibleRuleName(rule);
         runtimeProperties.putAll(convert(annotation.properties()));
      } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
         throw new RuntimeException(String.format("Failed instantiating: %s", clazz.getSimpleName()));
      }
   }

   @Override
   public Map<String, String> start() {
      try {
         return internalStart();
      } catch (Throwable t) {
         if (cacheManager != null) {
            log.error("Failed container output: \n{}", cacheManager.getLogs(OutputFrame.OutputType.STDOUT), t);
         } else {
            log.error("Cache manager was not initialized!", t);
         }
         throw t;
      }
   }

   @Override
   public void inject(TestInjector testInjector) {
      testInjector.injectIntoFields(cacheManager, new TestInjector.AnnotatedAndMatchesType(ContainerInject.class, HotRodContainer.class));
      testInjector.injectIntoFields(database, new TestInjector.AnnotatedAndMatchesType(ContainerInject.class, JdbcDatabaseContainer.class));
   }

   private Map<String, String> internalStart() {
      database = delegate.createDatabase();

      assert database != null : "Database can not be null";

      if (!database.isRunning()) database.start();

      installDatabaseDriver(database.getDriverClassName());
      Testcontainers.exposeHostPorts(database.getFirstMappedPort());
      String databaseKind = databaseKind(database.getJdbcUrl());
      cacheManager = createHotRodContainer(databaseKind);
      cacheManager.start();

      Map<String, String> properties = new HashMap<>(Map.of(
            "gingersnap.cache.uri", cacheManager.hotRodURI(),
            "gingersnap.database.host", database.getHost(),
            "gingersnap.database.port", Integer.toString(database.getFirstMappedPort()),
            "gingersnap.database.username", database.getUsername(),
            "gingersnap.database.password", database.getPassword()
      ));

      boolean isOracle = Profiles.isProfileActive("oracle");
      for (String rule : rules) {
         CountDownLatch prev = AUTOMATIC_STARTED_ENGINES.put(CacheIdentifier.of(rule, URI.create(cacheManager.hotRodURI())), new CountDownLatch(1));
         if (prev != null) prev.countDown();
         properties.putAll(Map.of(
               "gingersnap.rule.%s.connector.schema".formatted(rule), "debezium",
               "gingersnap.rule.%s.connector.table".formatted(rule), isOracle ? "CUSTOMER" : "customer",
               "gingersnap.rule.%s.key-columns".formatted(rule), isOracle ? "FULLNAME" : "fullname"
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

   private static void assertCompatibleRuleName(String name) {
      assert RULE_NAME_PATTERN.matcher(name).matches() : String.format("Rule '%s' is not a valid name", name);
   }

   private HotRodContainer<?> createHotRodContainer(String dbKind) {
      var clazz = Profiles.hotRodContainerClass();
      if (clazz.equals(InfinispanContainer.class)) {
         return new InfinispanContainer();
      }

      var url = "oracle".equals(dbKind) ?
            "oracle:thin:@host.testcontainers.internal:%s/%s".formatted(database.getFirstMappedPort(), database.getDatabaseName()) :
            "%s://host.testcontainers.internal:%s/%s".formatted(dbKind, database.getFirstMappedPort(), database.getDatabaseName());

      return new CacheManagerContainer(dbKind)
            .withDatabaseUrl(url)
            .withDatabaseUser(database.getUsername())
            .withDatabasePassword(database.getPassword())
            .withRules(rules);
   }

   private static void installDatabaseDriver(String className) {
      try {
         var driverClass = Thread.currentThread().getContextClassLoader().loadClass(className);
         assert Driver.class.isAssignableFrom(driverClass) : "Class " + className + " not instance of Driver";
         DriverManager.registerDriver((Driver) driverClass.getDeclaredConstructor().newInstance());
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   void listConnectorStarted(@Observes Events.ConnectorStartedEvent ev) {
      log.info("Received event for '{}'", ev.identifier());
      CountDownLatch latch = AUTOMATIC_STARTED_ENGINES.get(ev.identifier());
      if (latch != null) latch.countDown();
      else log.info("Could not find latch for '{}'", ev.identifier());
   }

   @Override
   public void beforeClass(Class<?> testClass) {
      var iterator = AUTOMATIC_STARTED_ENGINES.entrySet().iterator();
      while (iterator.hasNext()) {
         var entry = iterator.next();
         CacheIdentifier identifier = entry.getKey();
         CountDownLatch latch = entry.getValue();
         log.info("Waiting on connector '{}' to initialize", identifier);
         try {
            assert latch.await(25, TimeUnit.SECONDS) : String.format("Engine '%s' connector never started", identifier);
            log.info("Connector '{}' was initialized!", identifier);
         } catch (InterruptedException ignore) { }
         iterator.remove();
      }
   }
}
