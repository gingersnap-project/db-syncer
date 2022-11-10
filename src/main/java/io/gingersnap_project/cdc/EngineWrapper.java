package io.gingersnap_project.cdc;

import static io.debezium.relational.HistorizedRelationalDatabaseConnectorConfig.SCHEMA_HISTORY;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import io.gingersnap_project.cdc.cache.CacheService;
import io.gingersnap_project.cdc.chain.EventProcessingChain;
import io.gingersnap_project.cdc.chain.EventProcessingChainFactory;
import io.gingersnap_project.cdc.configuration.Backend;
import io.gingersnap_project.cdc.configuration.Connector;
import io.gingersnap_project.cdc.configuration.Database;
import io.gingersnap_project.cdc.configuration.Region;
import io.gingersnap_project.cdc.connector.DatabaseProvider;
import io.gingersnap_project.cdc.consumer.BatchConsumer;
import io.gingersnap_project.cdc.remote.RemoteOffsetStore;
import io.gingersnap_project.cdc.remote.RemoteSchemaHistory;
import io.gingersnap_project.cdc.translation.ColumnJsonTranslator;
import io.gingersnap_project.cdc.translation.ColumnStringTranslator;
import io.gingersnap_project.cdc.translation.IdentityTranslator;
import io.gingersnap_project.cdc.translation.JsonTranslator;
import io.gingersnap_project.cdc.translation.PrependJsonTranslator;
import io.gingersnap_project.cdc.translation.PrependStringTranslator;

import io.debezium.embedded.Connect;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import org.apache.kafka.connect.source.SourceRecord;

public class EngineWrapper {

   private static final ExecutorService executor = Executors.newFixedThreadPool(Math.max(4, Runtime.getRuntime().availableProcessors() * 2), runnable ->
         new Thread(runnable, "engine"));
   private final String name;
   private final CacheService cacheService;
   private final ManagedEngine managedEngine;
   private final Region.SingleRegion region;
   private final Properties properties;
   private volatile DebeziumEngine<ChangeEvent<SourceRecord, SourceRecord>> engine;

   private EngineWrapper(String name, Region.SingleRegion region, Properties properties, CacheService cacheService,
         ManagedEngine managedEngine) {
      this.name = name;
      this.cacheService = cacheService;
      this.managedEngine = managedEngine;
      this.region = region;
      this.properties = properties;
   }

   public EngineWrapper(String name, Region.SingleRegion region, CacheService cacheService, ManagedEngine managedEngine) {
      this(name, region, defaultProperties(name, region), cacheService, managedEngine);
   }

   private static Properties defaultProperties(String name, Region.SingleRegion region) {
      Properties props = new Properties();
      props.setProperty("name", "engine");

      Connector connector = region.connector();
      Database database = region.database();
      // Required property
      props.setProperty("topic.prefix", name);

      // MySQL information
      props.setProperty("database.hostname", database.hostname());
      props.setProperty("database.port", String.valueOf(database.port()));
      props.setProperty("database.user", database.user());
      props.setProperty("database.password", database.password());
      props.setProperty("database.server.name", "gingersnap-eager");
      props.setProperty("snapshot.mode", "initial"); // Behavior when offset not available.

      // Additional configuration
      props.setProperty("tombstones.on.delete", "false"); // Emit single event on delete. Doc says it should be true when using Kafka.
      props.setProperty("converter.schemas.enable", "true"); // Include schema in events, we use to retrieve the key.

      Backend backend = region.backend();
      String uri = backend.uri().toString();
      props.setProperty(RemoteOffsetStore.URI_CACHE, uri);
      props.setProperty(RemoteOffsetStore.TOPIC_NAME, name);
      props.setProperty("offset.storage", RemoteOffsetStore.class.getCanonicalName());
      props.setProperty("offset.flush.interval.ms", "60000");
      props.setProperty(RemoteSchemaHistory.URI_CACHE, uri);
      props.setProperty(RemoteSchemaHistory.TOPIC_NAME, name);
      props.setProperty(SCHEMA_HISTORY.name(), RemoteSchemaHistory.class.getCanonicalName());

      DatabaseProvider provider = DatabaseProvider.valueOf(connector.connector());
      props.putAll(provider.databaseProperties(connector, database));

      return props;
   }

   public void start() {
      EventProcessingChain chain = EventProcessingChainFactory.create(region, createCacheBackend(name, region.backend()));
      this.engine = DebeziumEngine.create(Connect.class)
            .using(properties)
            .using(this.getClass().getClassLoader())
            .notifying(new BatchConsumer(this, chain, executor))
            .build();
      executor.submit(engine);
   }

   public void stop() throws IOException {
      engine.close();
      engine = null;
      cacheService.stop(region.backend().uri());
   }

   public void notifyError() {
      managedEngine.engineError(name);
   }

   public void shutdownCacheService() {
       cacheService.shutdown(region.backend().uri());
   }

   public CompletionStage<Boolean> cacheServiceAvailable() {
      return cacheService.reconnect(region.backend().uri());
   }

   public String getName() {
      return name;
   }

   private CacheBackend createCacheBackend(String name, Backend backend) {
      JsonTranslator<?> keyTranslator;
      JsonTranslator<?> valueTranslator = backend.columns().isPresent() ?
            new ColumnJsonTranslator(backend.columns().get()) : IdentityTranslator.getInstance();
      Optional<List<String>> optionalKeys = backend.keyColumns();
      // TODO: hardcoded value here
      List<String> columnsToUse = optionalKeys.orElse(List.of("id"));
      switch (backend.keyType()) {
         case PLAIN:
            ColumnStringTranslator stringTranslator = new ColumnStringTranslator(columnsToUse, backend.plainSeparator());
            keyTranslator = backend.prefixRuleName() ? new PrependStringTranslator(stringTranslator, name) : stringTranslator;
            break;
         case JSON:
            ColumnJsonTranslator jsonTranslator = new ColumnJsonTranslator(columnsToUse);
            // TODO: hardcoded value here
            keyTranslator = backend.prefixRuleName() ? new PrependJsonTranslator(jsonTranslator, backend.jsonRuleName(), name) : jsonTranslator;
            break;
         default:
            throw new IllegalArgumentException("Key type: " + backend.keyType() + " not supported!");
      }

      return cacheService.backendForURI(backend.uri(), keyTranslator, valueTranslator);
   }
}
