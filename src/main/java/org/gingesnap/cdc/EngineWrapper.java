package org.gingesnap.cdc;

import static io.debezium.relational.HistorizedRelationalDatabaseConnectorConfig.SCHEMA_HISTORY;

import java.io.IOException;
import java.net.URI;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.gingesnap.cdc.configuration.Connector;
import org.gingesnap.cdc.configuration.Database;
import org.gingesnap.cdc.connector.DatabaseProvider;
import org.gingesnap.cdc.consumer.BatchConsumer;
import org.gingesnap.cdc.remote.RemoteOffsetStore;
import org.gingesnap.cdc.remote.RemoteSchemaHistory;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;

public class EngineWrapper {

   private static final ExecutorService executor = Executors.newSingleThreadExecutor(runnable ->
         new Thread(runnable, "engine"));
   private final DebeziumEngine<ChangeEvent<String, String>> engine;

   private EngineWrapper(Properties properties, CacheBackend cache) {
      engine = DebeziumEngine.create(Json.class)
            .using(properties)
            .using(this.getClass().getClassLoader())
            .notifying(new BatchConsumer(cache))
            .build();
   }

   public EngineWrapper(Connector connector, Database database, CacheBackend cache) {
      this(defaultProperties(connector, database, cache.uriUsed()), cache);
   }

   private static Properties defaultProperties(Connector connector, Database database, URI uriToUse) {
      Properties props = new Properties();
      props.setProperty("name", "engine");

      DatabaseProvider provider = DatabaseProvider.valueOf(connector.connector());
      props.putAll(provider.databaseProperties(connector, database));

      // Required property
      props.setProperty("topic.prefix", connector.topic());

      // MySQL information
      props.setProperty("database.hostname", database.hostname());
      props.setProperty("database.port", String.valueOf(database.port()));
      props.setProperty("database.user", database.user());
      props.setProperty("database.password", database.password());
      props.setProperty("database.server.id", String.valueOf(Math.abs(new Random().nextInt())));
      props.setProperty("database.server.name", "gingersnap-eager");
      props.setProperty("snapshot.mode", "initial"); // Behavior when offset not available.

      // Additional configuration
      props.setProperty("tombstones.on.delete", "false"); // Emit single event on delete. Doc says it should be true when using Kafka.
      props.setProperty("converter.schemas.enable", "true"); // Include schema in events, we use to retrieve the key.

      // Apply filters
      props.setProperty("transforms", "filter");
      props.setProperty("transforms.filter.type", "io.debezium.transforms.Filter");
      props.setProperty("transforms.filter.language", "jsr223.groovy");
      String schemaRegex = String.format("/%s\\.%s\\.%s\\..*/", connector.topic(), connector.schema(), connector.table());
      props.setProperty("transforms.filter.condition",
            // The value is from table 'customer' and is something with `topic.prefix`.`database.dbname`.table configuration.
            "value.source.table == 'customer' && valueSchema.name ==~ " + schemaRegex);

      props.setProperty(RemoteOffsetStore.URI_CACHE, uriToUse.toString());
      props.setProperty("offset.storage", RemoteOffsetStore.class.getCanonicalName());
      props.setProperty("offset.flush.interval.ms", "60000");
      props.setProperty(RemoteSchemaHistory.URI_CACHE, uriToUse.toString());
      props.setProperty(SCHEMA_HISTORY.name(), RemoteSchemaHistory.class.getCanonicalName());

      return props;
   }

   public void start() {
      executor.submit(engine);
   }

   public void stop() throws IOException {
      engine.close();
   }
}
