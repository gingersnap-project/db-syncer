package io.gingersnapproject.cdc.cache.hotrod;

import java.util.concurrent.CompletionStage;

import javax.annotation.PostConstruct;
import javax.enterprise.event.Observes;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.multimap.MultimapCacheManager;
import org.infinispan.client.hotrod.multimap.RemoteMultimapCacheManagerFactory;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.commons.configuration.StringConfiguration;
import org.infinispan.commons.dataconversion.MediaType;

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
import io.quarkus.runtime.ShutdownEvent;

@ApplicationScoped
public class HotRodService implements CacheService {

   @Inject NotificationManager eventing;

   @Inject Configuration config;

   volatile RemoteCacheManager rcm;

   @PostConstruct
   public void init() {
      rcm = new RemoteCacheManager(config.cache().uri());
      createCaches();
      eventing.backendStartedEvent();
   }

   public void shutdown(@Observes ShutdownEvent e) {
      if (rcm != null) {
         rcm.stopAsync().whenComplete((ignore, t) -> {
            if (t != null) {
               eventing.backendFailedEvent(t);
            } else {
               eventing.backendStoppedEvent();
            }
         });
         rcm = null;
      }
   }

   private static final String OFFSET_CACHE_NAME = "___debezium-offset";
   private static final String SCHEMA_CACHE_NAME = "___debezium-schema";

   @Override
   public CompletionStage<Boolean> reconnect() {
      if (rcm == null) {
         throw new IllegalStateException("RemoteCacheManager not initialized");
      }
      return rcm.startAsync().handle((__, t) -> {
         boolean successful = t == null;
         if (successful) {
            createCaches();
            eventing.backendStartedEvent();
         }
         return successful;
      });
   }

   @Override
   public CompletionStage<Void> stop() {
      if (rcm == null) {
         throw new IllegalStateException("RemoteCacheManager not initialized");
      }
      return rcm.stopAsync();
   }

   @Override
   public CacheBackend backendForRule(String name, Rule rule) {
      JsonTranslator<?> valueTranslator = rule.valueColumns().isPresent() ?
            new ColumnJsonTranslator(rule.valueColumns().get()) : IdentityTranslator.getInstance();
      JsonTranslator<?> keyTranslator = switch (rule.keyType()) {
         case TEXT -> new ColumnStringTranslator(rule.keyColumns(), rule.plainSeparator());
         case JSON -> new ColumnJsonTranslator(rule.keyColumns());
         case UNRECOGNIZED -> throw new UnsupportedOperationException("Unimplemented case: " + rule.keyType());
         default -> throw new IllegalArgumentException("Unexpected value: " + rule.keyType());
      };

      if (rcm == null)
         throw new IllegalStateException("RemoteCacheManager not initialized");

      return new HotRodCacheBackend(rcm.getCache(name), keyTranslator, valueTranslator, eventing);
   }

   private void getOrCreateCacheBackendCache(String name, RemoteCacheManager rcm) {
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
