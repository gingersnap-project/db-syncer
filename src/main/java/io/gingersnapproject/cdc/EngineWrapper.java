package io.gingersnapproject.cdc;

import static io.debezium.relational.HistorizedRelationalDatabaseConnectorConfig.SCHEMA_HISTORY;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import io.gingersnapproject.cdc.cache.CacheIdentifier;
import io.gingersnapproject.cdc.cache.CacheService;
import io.gingersnapproject.cdc.chain.EventProcessingChain;
import io.gingersnapproject.cdc.chain.EventProcessingChainFactory;
import io.gingersnapproject.cdc.configuration.Configuration;
import io.gingersnapproject.cdc.configuration.Connector;
import io.gingersnapproject.cdc.configuration.Database;
import io.gingersnapproject.cdc.configuration.Rule;
import io.gingersnapproject.cdc.connector.DatabaseProvider;
import io.gingersnapproject.cdc.consumer.BatchConsumer;
import io.gingersnapproject.cdc.event.NotificationManager;
import io.gingersnapproject.cdc.remote.RemoteOffsetStore;
import io.gingersnapproject.cdc.remote.RemoteSchemaHistory;

import io.debezium.embedded.Connect;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import org.apache.kafka.connect.source.SourceRecord;

public class EngineWrapper {

   private static final ExecutorService executor = Executors.newFixedThreadPool(Math.max(4, Runtime.getRuntime().availableProcessors() * 2), runnable ->
         new Thread(runnable, "engine"));
   private final CacheIdentifier identifier;
   private final CacheService cacheService;
   private final Configuration config;
   private final Rule rule;
   private final Properties properties;
   private final NotificationManager eventing;
   private volatile DebeziumEngine<ChangeEvent<SourceRecord, SourceRecord>> engine;
   private volatile boolean stopped = false;

   private EngineWrapper(CacheIdentifier identifier, Configuration config, Rule rule, Properties properties, CacheService cacheService,
                         NotificationManager eventing) {
      this.identifier = identifier;
      this.cacheService = cacheService;
      this.config = config;
      this.rule = rule;
      this.eventing = eventing;
      this.properties = properties;
   }

   public EngineWrapper(CacheIdentifier identifier, Configuration config, Rule rule, CacheService cacheService, NotificationManager eventing) {
      this(identifier, config, rule, defaultProperties(identifier, config, rule), cacheService, eventing);
   }

   private static Properties defaultProperties(CacheIdentifier identifier, Configuration config, Rule rule) {
      String name = identifier.rule();
      Properties props = new Properties();
      props.setProperty("name", "engine");

      Connector connector = rule.connector();
      // Required property
      props.setProperty("topic.prefix", identifier.toString());

      // Database information
      Database database = config.database();
      props.setProperty("database.hostname", database.host());
      props.setProperty("database.port", String.valueOf(database.port()));
      props.setProperty("database.user", database.username());
      props.setProperty("database.password", database.password());
      props.setProperty("database.server.name", "gingersnap-eager");
      props.setProperty("snapshot.mode", "initial"); // Behavior when offset not available.

      // Additional configuration
      props.setProperty("tombstones.on.delete", "false"); // Emit single event on delete. Doc says it should be true when using Kafka.
      props.setProperty("converter.schemas.enable", "true"); // Include schema in events, we use to retrieve the key.

      String uri = identifier.uri().toString();
      props.setProperty(RemoteOffsetStore.URI_CACHE, uri);
      props.setProperty(RemoteOffsetStore.TOPIC_NAME, name);
      props.setProperty("offset.storage", RemoteOffsetStore.class.getCanonicalName());
      props.setProperty("offset.flush.interval.ms", "60000");
      props.setProperty(RemoteSchemaHistory.URI_CACHE, uri);
      props.setProperty(RemoteSchemaHistory.TOPIC_NAME, name);
      props.setProperty(SCHEMA_HISTORY.name(), RemoteSchemaHistory.class.getCanonicalName());

      DatabaseProvider provider = database.type();
      props.putAll(provider.databaseProperties(connector, database));

      return props;
   }

   public void start() throws IOException {
      CacheBackend c = cacheService.start(identifier, rule);
      EventProcessingChain chain = EventProcessingChainFactory.create(rule, c);
      this.engine = DebeziumEngine.create(Connect.class)
            .using(properties)
            .using(this.getClass().getClassLoader())
            .notifying(new BatchConsumer(this, chain, executor))
            .using(new DebeziumEngine.ConnectorCallback() {
               @Override
               public void taskStarted() {
                  eventing.connectorStarted(identifier, config.database().type());
               }

               @Override
               public void connectorStopped() {
                  eventing.connectorStopped(identifier);
                  cacheService.stop(identifier);
               }
            })
            .using((success, message, error) -> {
               if (error != null) eventing.connectorFailed(identifier, error);
            })
            .build();
      executor.submit(engine);
      stopped = false;
   }

   public void stop() throws IOException {
      if (!stopped) {
         stopped = true;
         engine.close();
         engine = null;
      }
   }

   public void notifyError(Throwable t) {
      eventing.connectorFailed(identifier, t);
   }

   public String getName() {
      return identifier.rule();
   }

}
