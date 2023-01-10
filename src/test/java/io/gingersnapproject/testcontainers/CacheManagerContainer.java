package io.gingersnapproject.testcontainers;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.Base58;
import org.testcontainers.utility.DockerImageName;

public class CacheManagerContainer extends GenericContainer<CacheManagerContainer> {

   private static final String QUARKUS_DATASOURCE_REACTIVE_URL = "QUARKUS_DATASOURCE_REACTIVE_URL";
   private String databaseUrl;
   private String dbUser;
   private String dbPassword;
   private String dbKind;
   private String[] rules;

   public CacheManagerContainer() {
      super(DockerImageName.parse("local-cache-manager:latest"));
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

   public CacheManagerContainer withDatabaseKind(String dbKind) {
      this.dbKind = dbKind;
      return self();
   }

   public CacheManagerContainer withRules(String ... rules) {
      this.rules = rules;
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
      withEnv(String.format("quarkus_datasource_%s_username", dbKind), dbUser);
      withEnv(String.format("quarkus_datasource_%s_password", dbKind), dbPassword);
      withEnv(String.format("quarkus_datasource_mysql_reactive_url", dbKind), "");
      withEnv(String.format("quarkus_datasource_%s_reactive_url", dbKind), databaseUrl);


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
