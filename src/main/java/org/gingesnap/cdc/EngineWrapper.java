package org.gingesnap.cdc;

import static io.debezium.relational.HistorizedRelationalDatabaseConnectorConfig.SCHEMA_HISTORY;
import static io.debezium.storage.file.history.FileSchemaHistory.FILE_PATH;

import java.io.IOException;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.gingesnap.cdc.configuration.Connector;
import org.gingesnap.cdc.configuration.Database;
import org.gingesnap.cdc.consumer.BatchConsumer;
import org.infinispan.client.hotrod.RemoteCache;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import io.debezium.storage.file.history.FileSchemaHistory;

public class EngineWrapper {

   private static final ExecutorService executor = Executors.newSingleThreadExecutor(new ThreadFactory() {
      @Override
      public Thread newThread(Runnable runnable) {
         return new Thread(runnable, "engine");
      }
   });
   private final DebeziumEngine<ChangeEvent<String, String>> engine;

   private EngineWrapper(Properties properties, RemoteCache<String, String> cache) {
      engine = DebeziumEngine.create(Json.class)
            .using(properties)
            .using(this.getClass().getClassLoader())
            .notifying(new BatchConsumer(cache))
            .build();
   }

   public EngineWrapper(Connector connector, Database database, RemoteCache<String, String> cache) {
      this(defaultProperties(connector, database), cache);
   }

   private static Properties defaultProperties(Connector connector, Database database) {
      Properties props = new Properties();
      props.setProperty("name", "engine");
      props.put("connector.class", connector.connector());

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
      props.setProperty("table.include.list", String.format("%s.%s", connector.schema(), connector.table()));

      // Apply filters
      props.setProperty("transforms", "filter");
      props.setProperty("transforms.filter.type", "io.debezium.transforms.Filter");
      props.setProperty("transforms.filter.language", "jsr223.groovy");
      String schemaRegex = String.format("/%s\\.%s\\.%s\\..*/", connector.topic(), connector.schema(), connector.table());
      props.setProperty("transforms.filter.condition",
            // The value is from table 'customer' and is something with `topic.prefix`.`database.dbname`.table configuration.
            "value.source.table == 'customer' && valueSchema.name ==~ " + schemaRegex);

      /*props.setProperty("offset.storage", RemoteOffsetStore.class.getCanonicalName());
      props.setProperty(SCHEMA_HISTORY.name(), RemoteSchemaHistory.class.getCanonicalName());*/

      props.setProperty("offset.storage", FileOffsetBackingStore.class.getCanonicalName());
      props.setProperty("offset.storage.file.filename", "/tmp/offset.dat");
      props.setProperty(SCHEMA_HISTORY.name(), FileSchemaHistory.class.getCanonicalName());
      props.setProperty(FILE_PATH.name(), "/tmp/schema.dat");

      return props;
   }

   public void start() {
      executor.submit(engine);
   }

   public void stop() throws IOException {
      engine.close();
   }
}
