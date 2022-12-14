package io.gingersnapproject.testcontainers;

import static io.gingersnapproject.testcontainers.BaseGingersnapResourceLifecycleManager.databaseKind;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.Base58;
import org.testcontainers.utility.DockerImageName;

public class CacheManagerContainer extends GenericContainer<CacheManagerContainer> {

   private static final String QUARKUS_DATASOURCE_REACTIVE_URL = "QUARKUS_DATASOURCE_REACTIVE_URL";
   private static final String QUARKUS_DATASOURCE_DBKIND = "QUARKUS_DATASOURCE_DBKIND";
   private String databaseUrl;

   public CacheManagerContainer() {
      super(DockerImageName.parse("quay.io/gingersnap/cache-manager:latest"));
      withNetworkAliases("infinispan-" + Base58.randomString(6));
      withExposedPorts(8080, 11222);
      waitingFor(Wait.forHttp("/q/health").forPort(8080));
   }

   public CacheManagerContainer withDatabaseUrl(String databaseUrl) {
      this.databaseUrl = databaseUrl;
      return self();
   }

   public int hotrodPort() {
      return getMappedPort(11222);
   }

   public int restPort() {
      return getMappedPort(8080);
   }

   public String hotrodUri() {
      return String.format("hotrod://%s:%d", getHost(), hotrodPort());
   }

   @Override
   public void start() {
      withEnv(QUARKUS_DATASOURCE_REACTIVE_URL, databaseUrl);
      withEnv(QUARKUS_DATASOURCE_DBKIND, databaseKind(databaseUrl));
      super.start();
   }
}
