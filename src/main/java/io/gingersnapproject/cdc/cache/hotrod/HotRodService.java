package io.gingersnapproject.cdc.cache.hotrod;

import io.gingersnapproject.cdc.CacheBackend;
import io.gingersnapproject.cdc.OffsetBackend;
import io.gingersnapproject.cdc.SchemaBackend;
import io.gingersnapproject.cdc.cache.CacheService;
import io.gingersnapproject.cdc.configuration.Configuration;
import io.gingersnapproject.cdc.configuration.Rule;
import io.gingersnapproject.cdc.event.NotificationManager;
import io.gingersnapproject.cdc.translation.ColumnJsonTranslator;
import io.gingersnapproject.cdc.translation.ColumnStringTranslator;
import io.gingersnapproject.cdc.translation.IdentityTranslator;
import io.gingersnapproject.cdc.translation.JsonTranslator;
import io.gingersnapproject.metrics.DBSyncerMetrics;

import io.quarkus.runtime.ShutdownEvent;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.multimap.MultimapCacheManager;
import org.infinispan.client.hotrod.multimap.RemoteMultimapCacheManagerFactory;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.commons.configuration.StringConfiguration;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.util.concurrent.CompletableFutures;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

@ApplicationScoped
public class HotRodService implements CacheService {
   private static final String OFFSET_CACHE_NAME = "___debezium-offset";
   private static final String SCHEMA_CACHE_NAME = "___debezium-schema";

   @Inject NotificationManager eventing;

   @Inject Configuration config;

   @Inject DBSyncerMetrics metrics;

   volatile RemoteCacheManager rcm;

   private final Map<String, CacheBackend> backends = new ConcurrentHashMap<>();
   private volatile boolean shutdownSignal;

   @PostConstruct
   public void init() {
      rcm = new RemoteCacheManager(config.cache().uri());
      createCaches();
   }

   public void shutdown(@Observes ShutdownEvent ignore) {
      if (rcm != null) {
         shutdownSignal = true;
         if (backends.values().stream().noneMatch(CacheBackend::isRunning)) destroy();
      }
   }

   @Override
   public CompletionStage<Boolean> reconnect(String name, Rule rule) {
      if (!backends.containsKey(name)) return CompletableFutures.completedFalse();

      return CompletableFuture.supplyAsync(() -> {
         backendForRule(name, rule).start();
         return null;
      }).handle((ignore, t) -> {
         if (t != null) {
            eventing.backendFailedEvent(name, t);
            return false;
         }
         createCaches();
         return true;
      });
   }

   @Override
   public void stop(String name) {
      if (!backends.containsKey(name)) throw new IllegalStateException(String.format("Backend for %s not created", name));
      backends.computeIfPresent(name, (ignore, cache) -> {
         cache.stop();
         return cache;
      });

      if (shutdownSignal) destroy();
   }

   private void destroy() {
      CompletionStage<Void> cs = rcm.stopAsync();
      rcm = null;
      CompletableFutures.uncheckedAwait(cs.toCompletableFuture());
   }

   @Override
   public CacheBackend backendForRule(String name, Rule rule) {
      return backends.computeIfAbsent(name, ignore -> createBackend(name, rule));
   }

   private CacheBackend createBackend(String name, Rule rule) {
      JsonTranslator<?> valueTranslator = rule.valueColumns().isPresent() ?
            new ColumnJsonTranslator(rule.valueColumns().get()) : IdentityTranslator.getInstance();
      JsonTranslator<?> keyTranslator = switch (rule.keyType()) {
         case TEXT -> new ColumnStringTranslator(rule.keyColumns(), rule.plainSeparator());
         case JSON -> new ColumnJsonTranslator(rule.keyColumns());
         case UNRECOGNIZED -> throw new UnsupportedOperationException("Unimplemented case: " + rule.keyType());
      };

      if (rcm == null)
         throw new IllegalStateException("RemoteCacheManager not initialized");

      getOrCreateCacheBackendCache(name, rcm);
      var cache = new HotRodCacheBackend(rcm.getCache(name), keyTranslator, valueTranslator, eventing, metrics);
      cache.start();
      return cache;
   }

   private static void getOrCreateCacheBackendCache(String name, RemoteCacheManager rcm) {
      rcm.administration()
            .withFlags(CacheContainerAdmin.AdminFlag.VOLATILE)
            .getOrCreateCache(name, new StringConfiguration(
            "<local-cache>" +
               "<encoding>" +
                  "<key media-type=\"" + MediaType.TEXT_PLAIN_TYPE + "\"/>" +
                  "<value media-type=\"" + MediaType.TEXT_PLAIN_TYPE + "\"/>" +
               "</encoding>" +
            "</local-cache>"));
   }

   @Override
   public OffsetBackend offsetBackend() {
      return new HotRodOffsetBackend(rcm.getCache(OFFSET_CACHE_NAME));
   }

   private void getOrCreateOffsetBackendCache(String name, RemoteCacheManager rcm) {
      rcm.administration()
            .withFlags(CacheContainerAdmin.AdminFlag.VOLATILE)
            .getOrCreateCache(name, new StringConfiguration(
            "<local-cache>" +
               "<encoding media-type=\"" + MediaType.APPLICATION_OCTET_STREAM_TYPE + "\"/>" +
            "</local-cache>"));
   }

   @Override
   public SchemaBackend schemaBackend() {
      getOrCreateSchemaBackendCache(SCHEMA_CACHE_NAME, rcm);
      MultimapCacheManager<String, String> remoteMultimapCacheManager = RemoteMultimapCacheManagerFactory.from(rcm);
      // Support duplicates so it uses a list which is ordered
      return new HotRodSchemaBackend(remoteMultimapCacheManager.get(SCHEMA_CACHE_NAME, true));
   }

   private RemoteCache<String, String> getOrCreateSchemaBackendCache(String multiMapCacheName, RemoteCacheManager rcm) {
      return rcm.administration()
            .withFlags(CacheContainerAdmin.AdminFlag.VOLATILE)
            .getOrCreateCache(multiMapCacheName, new StringConfiguration(
            "<local-cache>" +
               "<encoding>" +
                  "<key media-type=\"" + MediaType.TEXT_PLAIN_TYPE + "\"/>" +
                  "<value media-type=\"" + MediaType.APPLICATION_OCTET_STREAM_TYPE + "\"/>" +
               "</encoding>" +
            "</local-cache>"));
   }

   private void createCaches() {
      config.rules().keySet().forEach(r -> getOrCreateCacheBackendCache(r, rcm));
      getOrCreateOffsetBackendCache(OFFSET_CACHE_NAME, rcm);
      getOrCreateSchemaBackendCache(SCHEMA_CACHE_NAME, rcm);
   }
}
