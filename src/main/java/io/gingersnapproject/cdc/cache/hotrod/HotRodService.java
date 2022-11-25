package io.gingersnapproject.cdc.cache.hotrod;

import java.net.URI;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.inject.Inject;
import javax.inject.Singleton;

import io.gingersnapproject.cdc.CacheBackend;
import io.gingersnapproject.cdc.OffsetBackend;
import io.gingersnapproject.cdc.SchemaBackend;
import io.gingersnapproject.cdc.cache.CacheService;
import io.gingersnapproject.cdc.event.NotificationManager;
import io.gingersnapproject.cdc.translation.JsonTranslator;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.multimap.MultimapCacheManager;
import org.infinispan.client.hotrod.multimap.RemoteMultimapCacheManagerFactory;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.commons.configuration.StringConfiguration;
import org.infinispan.commons.dataconversion.MediaType;

import io.quarkus.arc.lookup.LookupIfProperty;

@LookupIfProperty(name = "service.hotrod.enabled", stringValue = "true", lookupIfMissing = true)
@Singleton
public class HotRodService implements CacheService {

   @Inject NotificationManager eventing;
   ConcurrentMap<URI, RemoteCacheManager> otherURIs = new ConcurrentHashMap<>();
   private static final String CACHE_NAME = "debezium-cache";
   private static final String OFFSET_CACHE_NAME = "debezium-offset";
   private static final String SCHEMA_CACHE_NAME = "debezium-schema";

   @Override
   public boolean supportsURI(URI uri) {
      return uri.getScheme().startsWith("hotrod");
   }

   @Override
   public CompletionStage<Boolean> reconnect(URI uri) {
      RemoteCacheManager rcm = otherURIs.get(uri);
      if (rcm == null) {
         throw new IllegalStateException("URI " + uri + " not present to restart!");
      }
      return rcm.startAsync().handle((__, t) -> {
         boolean successful = t == null;
         if (successful) {
            createCaches(rcm);
            eventing.backendStartedEvent(uri);
         }
         return successful;
      });
   }

   @Override
   public CompletionStage<Void> stop(URI uri) {
      RemoteCacheManager rcm = otherURIs.get(uri);
      if (rcm == null) {
         throw new IllegalStateException("URI " + uri + " not present to stop!");
      }
      return rcm.stopAsync();
   }

   private RemoteCacheManager managerForURI(URI uri) {
      return otherURIs.computeIfAbsent(uri, this::createManagerForURI);
   }

   private RemoteCacheManager createManagerForURI(URI uri) {
      try {
         RemoteCacheManager remoteCacheManager = new RemoteCacheManager(uri);
         createCaches(remoteCacheManager);
         eventing.backendStartedEvent(uri);
         return remoteCacheManager;
      } catch (Exception e) {
         eventing.backendFailedEvent(uri, e);
         return null;
      }
   }

   @Override
   public CacheBackend backendForURI(URI uri, JsonTranslator<?> keyTranslator, JsonTranslator<?> valueTranslator) {
      RemoteCacheManager remoteCacheManager = managerForURI(uri);
      if (remoteCacheManager == null) return null;
      return new HotRodCacheBackend(uri, remoteCacheManager.getCache(CACHE_NAME), keyTranslator, valueTranslator, eventing);
   }

   private void getOrCreateCacheBackendCache(RemoteCacheManager rcm) {
      rcm.administration()
            .withFlags(CacheContainerAdmin.AdminFlag.VOLATILE)
            .getOrCreateCache(CACHE_NAME, new StringConfiguration(
            "<local-cache>" +
               "<encoding>" +
                  "<key media-type=\"" + MediaType.TEXT_PLAIN_TYPE + "\"/>" +
                  "<value media-type=\"" + MediaType.TEXT_PLAIN_TYPE + "\"/>" +
               "</encoding>" +
            "</local-cache>"));
   }

   @Override
   public OffsetBackend offsetBackendForURI(URI uri) {
      RemoteCacheManager remoteCacheManager = managerForURI(uri);
      if (remoteCacheManager == null) return null;
      return new HotRodOffsetBackend(remoteCacheManager.getCache(OFFSET_CACHE_NAME));
   }

   private void getOrCreateOffsetBackendCache(RemoteCacheManager rcm) {
      rcm.administration()
            .withFlags(CacheContainerAdmin.AdminFlag.VOLATILE)
            .getOrCreateCache(OFFSET_CACHE_NAME, new StringConfiguration(
            "<local-cache>" +
               "<encoding media-type=\"" + MediaType.APPLICATION_OCTET_STREAM_TYPE + "\"/>" +
            "</local-cache>"));
   }

   @Override
   public SchemaBackend schemaBackendForURI(URI uri) {
      RemoteCacheManager remoteCacheManager = managerForURI(uri);
      if (remoteCacheManager == null) return null;
      getOrCreateSchemaBackendCache(remoteCacheManager, SCHEMA_CACHE_NAME);
      MultimapCacheManager<String, String> remoteMultimapCacheManager = RemoteMultimapCacheManagerFactory.from(remoteCacheManager);
      // Support duplicates so it uses a list which is ordered
      return new HotRodSchemaBackend(remoteMultimapCacheManager.get(SCHEMA_CACHE_NAME, true));
   }

   private RemoteCache<String, String> getOrCreateSchemaBackendCache(RemoteCacheManager rcm, String multiMapCacheName) {
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

   private void createCaches(RemoteCacheManager remoteCacheManager) {
      getOrCreateCacheBackendCache(remoteCacheManager);
      getOrCreateOffsetBackendCache(remoteCacheManager);
      getOrCreateSchemaBackendCache(remoteCacheManager, SCHEMA_CACHE_NAME);
   }

   @Override
   public void shutdown(URI uri) {
      RemoteCacheManager rcm = otherURIs.remove(uri);
      if (rcm != null) {
         rcm.stopAsync().whenComplete((ignore, t) -> {
            if (t != null) {
               eventing.backendFailedEvent(uri, t);
            } else {
               eventing.backendStoppedEvent(uri);
            }
         });
      }
   }
}
