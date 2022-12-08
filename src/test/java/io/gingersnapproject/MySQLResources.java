package io.gingersnapproject;

import java.util.Map;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.Base58;
import org.testcontainers.utility.MountableFile;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

public class MySQLResources implements QuarkusTestResourceLifecycleManager {

   private GenericContainer<?> cacheManager;
   private MySQLContainer<?> db;

   @Override
   public Map<String, String> start() {
      cacheManager = new GenericContainer<>("quay.io/gingersnap/cache-manager")
            .withNetworkAliases("infinispan-" + Base58.randomString(6))
            .withExposedPorts(8080, 11222)
            .waitingFor(Wait.forHttp("/q/health").forPort(8080));
      cacheManager.start();

      db = new MySQLContainer<>("mysql:latest")
            .withUsername("gingersnap_user")
            .withPassword("password")
            .withExposedPorts(3306)
            .withCopyFileToContainer(MountableFile.forClasspathResource("setup.sql"), "/docker-entrypoint-initdb.d/setup.sql");
      db.start();

      return Map.of(
            "gingersnap.cache.uri", String.format("hotrod://%s:%d", cacheManager.getHost(), cacheManager.getMappedPort(11222)),
            "gingersnap.database.type", "MYSQL",
            "gingersnap.database.host", db.getHost(),
            "gingersnap.database.user", db.getUsername(),
            "gingersnap.database.password", db.getPassword(),
            "gingersnap.database.port", Integer.toString(db.getMappedPort(3306)),
            "gingersnap.rule.us-east.connector.schema", "debezium",
            "gingersnap.rule.us-east.connector.table", "customer",
            "gingersnap.rule.us-east.key-columns", "fullname"
      );
   }

   @Override
   public void stop() {
      if (cacheManager != null)
         cacheManager.stop();

      if (db != null)
         db.stop();
   }
}
