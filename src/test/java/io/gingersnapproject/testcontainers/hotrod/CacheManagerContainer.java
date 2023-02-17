package io.gingersnapproject.testcontainers.hotrod;

import java.net.URI;

import io.restassured.response.ValidatableResponse;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.Base58;
import org.testcontainers.utility.DockerImageName;

public class CacheManagerContainer extends HotRodContainer<CacheManagerContainer> {

   private final String dbKind;
   private String databaseUrl;
   private String dbUser;
   private String dbPassword;
   private String[] rules;

   public CacheManagerContainer(String kind) {
      super(DockerImageName.parse(String.format("quay.io/gingersnap/cache-manager-%s:latest", kind)));
      this.dbKind = kind;
      withNetworkAliases("infinispan-" + Base58.randomString(6));
      withExposedPorts(8080, 11222);
      waitingFor(Wait.forHttp("/q/health").forPort(8080));
   }

   public CacheManagerContainer withDatabaseUrl(String databaseUrl) {
      this.databaseUrl = databaseUrl;
      return self();
   }

   public CacheManagerContainer withDatabaseUser(String dbUser) {
      this.dbUser = dbUser;
      return self();
   }

   public CacheManagerContainer withDatabasePassword(String dbPassword) {
      this.dbPassword = dbPassword;
      return self();
   }

   public CacheManagerContainer withRules(String ... rules) {
      this.rules = rules;
      return self();
   }

   @Override
   public void start() {
      withEnv("quarkus.datasource.username", dbUser);
      withEnv("quarkus.datasource.password", dbPassword);
      withEnv("quarkus.datasource.reactive.url", databaseUrl);


      if (rules != null) {
         for (String rule : rules) {
            // The where clause should use the same column the db-syncer uses.
            withEnv("gingersnap.eager-rule.%s.select-statement".formatted(rule), "select fullname, email from debezium.customer where fullname = ?");

            // If cache-manager start using Oracle's approach of uppercase, this needs to be updated.
            withEnv("gingersnap.eager-rule.%s.connector.schema".formatted(rule), "debezium");
            withEnv("gingersnap.eager-rule.%s.connector.table".formatted(rule), "customer");
         }
      }

      super.start();
   }

   @Override
   public ValidatableResponse getById(String rule, String id) {
      return getById(URI.create(String.format("http://%s:%d/rules/%s/%s", getHost(), getMappedPort(8080), rule, id)));
   }
}
