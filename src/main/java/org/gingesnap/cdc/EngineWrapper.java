package org.gingesnap.cdc;

import static io.debezium.relational.HistorizedRelationalDatabaseConnectorConfig.SCHEMA_HISTORY;

import java.io.IOException;
import java.net.URI;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.gingesnap.cdc.cache.CacheService;
import org.gingesnap.cdc.configuration.Connector;
import org.gingesnap.cdc.configuration.Database;
import org.gingesnap.cdc.connector.DatabaseProvider;
import org.gingesnap.cdc.consumer.BatchConsumer;
import org.gingesnap.cdc.remote.RemoteOffsetStore;
import org.gingesnap.cdc.remote.RemoteSchemaHistory;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import io.debezium.storage.file.history.FileSchemaHistory;

public class EngineWrapper {

   private static final ExecutorService executor = Executors.newSingleThreadExecutor(runnable ->
         new Thread(runnable, "engine"));
   private final String name;
   private final URI uri;
   private final CacheService cacheService;
   private final ManagedEngine managedEngine;
   private final Properties properties;
   private volatile DebeziumEngine<ChangeEvent<String, String>> engine;

   private EngineWrapper(String name, URI uri, Properties properties, CacheService cacheService, ManagedEngine managedEngine) {
      this.name = name;
      this.uri = uri;
      this.cacheService = cacheService;
      this.managedEngine = managedEngine;
      this.properties = properties;
   }

   public EngineWrapper(String name, URI uri, Connector connector, Database database, CacheService cacheService, ManagedEngine engine) {
      this(name, uri, defaultProperties(name, connector, database, uri), cacheService, engine);
   }

   private static Properties defaultProperties(String name, Connector connector, Database database, URI uriToUse) {
      Properties props = new Properties();
      props.setProperty("name", "engine");

      // Required property
      props.setProperty("topic.prefix", name);

      // MySQL information
      props.setProperty("database.hostname", database.hostname());
      props.setProperty("database.port", String.valueOf(database.port()));
      props.setProperty("database.user", database.user());
      props.setProperty("database.password", database.password());
      props.setProperty("database.server.name", "gingersnap-eager");
      props.setProperty("snapshot.mode", "when_needed"); // Behavior when offset not available.

      // Additional configuration
      props.setProperty("tombstones.on.delete", "false"); // Emit single event on delete. Doc says it should be true when using Kafka.
      props.setProperty("converter.schemas.enable", "true"); // Include schema in events, we use to retrieve the key.

      // Apply filters
      props.setProperty("transforms", "filter");
      props.setProperty("transforms.filter.type", "io.debezium.transforms.Filter");
      props.setProperty("transforms.filter.language", "jsr223.groovy");
      String schemaRegex = String.format("/%s\\.%s\\.%s\\..*/", name, connector.schema(), connector.table());
      props.setProperty("transforms.filter.condition",
            // The value is from table 'customer' and is something with `topic.prefix`.`database.dbname`.table configuration.
            "value.source.table == 'customer' && valueSchema.name ==~ " + schemaRegex);

      props.setProperty(RemoteOffsetStore.URI_CACHE, uriToUse.toString());
      props.setProperty(RemoteOffsetStore.TOPIC_NAME, name);
      props.setProperty("offset.storage", RemoteOffsetStore.class.getCanonicalName());
      props.setProperty("offset.flush.interval.ms", "60000");
      props.setProperty(RemoteSchemaHistory.URI_CACHE, uriToUse.toString());
      props.setProperty(RemoteSchemaHistory.TOPIC_NAME, name);
      props.setProperty(SCHEMA_HISTORY.name(), RemoteSchemaHistory.class.getCanonicalName());

      DatabaseProvider provider = DatabaseProvider.valueOf(connector.connector());
      props.putAll(provider.databaseProperties(connector, database));

      return props;
   }

   public void start() {
      this.engine = DebeziumEngine.create(Json.class)
            .using(properties)
            .using(this.getClass().getClassLoader())
            .notifying(new BatchConsumer(cacheService.backendForURI(uri), this))
            .build();
      executor.submit(engine);
   }

   public void stop() throws IOException {
      engine.close();
      engine = null;
      cacheService.stop(uri);
   }

   public void notifyError() {
      managedEngine.engineError(name);
   }

   public void shutdownCacheService() {
       cacheService.shutdown(uri);
   }

   public CompletionStage<Boolean> cacheServiceAvailable() {
      return cacheService.reconnect(uri);
   }

   public String getName() {
      return name;
   }
}
