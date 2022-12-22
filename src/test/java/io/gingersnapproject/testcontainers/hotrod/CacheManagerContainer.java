package io.gingersnapproject.testcontainers.hotrod;

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
      withEnv("quarkus_datasource_username", dbUser);
      withEnv("quarkus_datasource_password", dbPassword);
      withEnv("quarkus_datasource_reactive_url", databaseUrl);


      if (rules != null) {
         for (String rule : rules) {
            // The where clause should use the same column the db-syncer uses.
            withEnv(String.format("gingersnap_rule_%s_select_statement", rule), "select fullname, email from customer where fullname = ?");
            withEnv(String.format("gingersnap_rule_%s_connector_schema", rule), "debezium");
            withEnv(String.format("gingersnap_rule_%s_connector_table", rule), "customer");
         }
      }

      super.start();
   }
}
