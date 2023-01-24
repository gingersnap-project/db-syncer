package io.gingersnapproject.cdc.cache.hotrod;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import io.gingersnapproject.cdc.CacheBackend;
import io.gingersnapproject.cdc.OffsetBackend;
import io.gingersnapproject.cdc.SchemaBackend;
import io.gingersnapproject.cdc.cache.CacheIdentifier;
import io.gingersnapproject.cdc.cache.CacheService;
import io.gingersnapproject.cdc.configuration.Rule;
import io.gingersnapproject.cdc.event.NotificationManager;
import io.gingersnapproject.cdc.translation.ColumnJsonTranslator;
import io.gingersnapproject.cdc.translation.ColumnStringTranslator;
import io.gingersnapproject.cdc.translation.IdentityTranslator;
import io.gingersnapproject.cdc.translation.JsonTranslator;
import io.gingersnapproject.metrics.DBSyncerMetrics;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.multimap.MultimapCacheManager;
import org.infinispan.client.hotrod.multimap.RemoteMultimapCacheManagerFactory;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.commons.configuration.StringConfiguration;
import org.infinispan.commons.dataconversion.MediaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class HotRodService implements CacheService {
   private static final Logger log = LoggerFactory.getLogger(HotRodService.class);
   private static final String OFFSET_CACHE_NAME = "___debezium-offset";
   private static final String SCHEMA_CACHE_NAME = "___debezium-schema";

   @Inject NotificationManager eventing;

   @Inject DBSyncerMetrics metrics;

   private final Map<URI, HotRodCache> managers = new ConcurrentHashMap<>();

   @Override
   public CompletionStage<Boolean> reconnect(CacheIdentifier identifier, Rule rule) {
      var cache = getCacheOrThrow(identifier);
      return cache.rcm.startAsync().thenApply(ignore -> {
         var backend = cache.backends.get(identifier.rule());
         if (backend == null) throw new IllegalStateException(String.format("Backend not found for '%s'", identifier));
         return backend.reconnect();
      });
   }

   @Override
   public void stop(CacheIdentifier identifier) {
      managers.computeIfPresent(identifier.uri(), (ignore, cache) -> {
         var backend = cache.backends.get(identifier.rule());

         if (backend != null) backend.stop();
         if (cache.backends.values().stream().noneMatch(CacheBackend::isRunning)) {
            cache.rcm.stopAsync().whenComplete((___, t) -> {
               if (t != null) {
                  log.error("Exception while stopping manager {}", identifier, t);
               }
            });
         }

         return cache;
      });
   }

   @Override
   public CacheBackend backendForRule(CacheIdentifier identifier, Rule rule) {
      var cache = managers.computeIfAbsent(identifier.uri(), ignore -> createBackend(identifier));
      return cache.backends.computeIfAbsent(identifier.rule(), ignore -> createBackend(cache.rcm, identifier.rule(), rule));
   }

   private HotRodCache createBackend(CacheIdentifier identifier) {
      var rcm = new RemoteCacheManager(identifier.uri());
      getOrCreateOffsetBackendCache(OFFSET_CACHE_NAME, rcm);
      getOrCreateSchemaBackendCache(SCHEMA_CACHE_NAME, rcm);

      var offset = new HotRodOffsetBackend(rcm.getCache(OFFSET_CACHE_NAME));
      MultimapCacheManager<String, String> remoteMultimapCacheManager = RemoteMultimapCacheManagerFactory.from(rcm);
      // Support duplicates so it uses a list which is ordered
      var schema = new HotRodSchemaBackend(remoteMultimapCacheManager.get(SCHEMA_CACHE_NAME, true));
      return new HotRodCache(identifier, rcm, new HashMap<>(), offset, schema);
   }

   private CacheBackend createBackend(RemoteCacheManager rcm, String name, Rule rule) {
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
   public OffsetBackend offsetBackend(URI managerURI) {
      return getCacheOrThrow(managerURI).offset();
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
   public SchemaBackend schemaBackend(URI managerURI) {
      return getCacheOrThrow(managerURI).schema();
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

   private HotRodCache getCacheOrThrow(CacheIdentifier identifier) {
      return getCacheOrThrow(identifier.uri());
   }

   private HotRodCache getCacheOrThrow(URI identifier) {
      var cache = managers.get(identifier);
      if (cache == null) throw new IllegalStateException(String.format("Cache not found for '%s'", identifier));

      return cache;
   }

   private record HotRodCache(
         CacheIdentifier identifier,
         RemoteCacheManager rcm,
         Map<String, CacheBackend> backends,
         OffsetBackend offset,
         SchemaBackend schema
   ) { }
}
