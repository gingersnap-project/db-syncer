package io.gingersnapproject.cdc;

import static io.debezium.relational.HistorizedRelationalDatabaseConnectorConfig.SCHEMA_HISTORY;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.embedded.Connect;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
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

public class EngineWrapper {

   private static final ExecutorService executor = Executors.newFixedThreadPool(Math.max(4, Runtime.getRuntime().availableProcessors() * 2), runnable ->
         new Thread(runnable, "engine"));
   private final String name;
   private final CacheService cacheService;
   private final Configuration config;
   private final Rule rule;
   private final Properties properties;
   private final NotificationManager eventing;
   private volatile DebeziumEngine<ChangeEvent<SourceRecord, SourceRecord>> engine;

   private EngineWrapper(String name, Configuration config, Rule rule, Properties properties, CacheService cacheService,
                         NotificationManager eventing) {
      this.name = name;
      this.cacheService = cacheService;
      this.config = config;
      this.rule = rule;
      this.eventing = eventing;
      this.properties = properties;
   }

   public EngineWrapper(String name, Configuration config, Rule rule, CacheService cacheService, NotificationManager eventing) {
      this(name, config, rule, defaultProperties(name, config, rule), cacheService, eventing);
   }

   private static Properties defaultProperties(String name, Configuration config, Rule rule) {
      Properties props = new Properties();
      props.setProperty("name", "engine");

      Connector connector = rule.connector();
      // Required property
      props.setProperty("topic.prefix", name);

      // Database information
      Database database = config.database();
      props.setProperty("database.hostname", database.host());
      props.setProperty("database.port", String.valueOf(database.port()));
      props.setProperty("database.user", database.user());
      props.setProperty("database.password", database.password());
      props.setProperty("database.server.name", "gingersnap-eager");
      props.setProperty("snapshot.mode", "initial"); // Behavior when offset not available.

      // Additional configuration
      props.setProperty("tombstones.on.delete", "false"); // Emit single event on delete. Doc says it should be true when using Kafka.
      props.setProperty("converter.schemas.enable", "true"); // Include schema in events, we use to retrieve the key.

      String uri = config.cache().uri().toString();
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

   public void start() {
      CacheBackend c = createCacheBackend(name, rule);
      if (c != null) {
         EventProcessingChain chain = EventProcessingChainFactory.create(rule, c);
         this.engine = DebeziumEngine.create(Connect.class)
               .using(properties)
               .using(this.getClass().getClassLoader())
               .notifying(new BatchConsumer(this, chain, executor))
               .using(new DebeziumEngine.ConnectorCallback() {
                  @Override
                  public void taskStarted() {
                     eventing.connectorStarted(name);
                  }

                  @Override
                  public void taskStopped() {
                     eventing.connectorStopped(name);
                  }
               })
               .using((success, message, error) -> {
                  if (error != null) eventing.connectorFailed(name, error);
               })
               .build();
         executor.submit(engine);
      }
   }

   public void stop() throws IOException {
      engine.close();
      engine = null;
      cacheService.stop(config.cache().uri());
   }

   public void notifyError(Throwable t) {
      eventing.connectorFailed(name, t);
   }

   public void shutdownCacheService() {
       cacheService.shutdown(config.cache().uri());
   }

   public CompletionStage<Boolean> cacheServiceAvailable() {
      return cacheService.reconnect(config.cache().uri());
   }

   public String getName() {
      return name;
   }

   private CacheBackend createCacheBackend(String name, Rule rule) {
      return cacheService.backendForRule(name, rule);
   }
}
