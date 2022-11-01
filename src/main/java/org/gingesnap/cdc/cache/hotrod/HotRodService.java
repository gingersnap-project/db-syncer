package org.gingesnap.cdc.cache.hotrod;

import java.net.URI;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.inject.Singleton;

import org.gingesnap.cdc.CacheBackend;
import org.gingesnap.cdc.OffsetBackend;
import org.gingesnap.cdc.SchemaBackend;
import org.gingesnap.cdc.cache.CacheService;
import org.gingesnap.cdc.translation.JsonTranslator;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.multimap.MultimapCacheManager;
import org.infinispan.client.hotrod.multimap.RemoteMultimapCacheManagerFactory;
import org.infinispan.commons.configuration.StringConfiguration;
import org.infinispan.commons.dataconversion.MediaType;

import io.quarkus.arc.lookup.LookupIfProperty;

@LookupIfProperty(name = "service.hotrod.enabled", stringValue = "true", lookupIfMissing = true)
@Singleton
public class HotRodService implements CacheService {
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
      return otherURIs.computeIfAbsent(uri, innerURI -> {
         RemoteCacheManager remoteCacheManager = new RemoteCacheManager(innerURI);
         createCaches(remoteCacheManager);
         return remoteCacheManager;
      });
   }

   @Override
   public CacheBackend backendForURI(URI uri, JsonTranslator<?> keyTranslator, JsonTranslator<?> valueTranslator) {
      RemoteCacheManager remoteCacheManager = managerForURI(uri);
      return new HotRodCacheBackend(uri, remoteCacheManager.getCache(CACHE_NAME), keyTranslator, valueTranslator);
   }

   private void getOrCreateCacheBackendCache(RemoteCacheManager rcm) {
      rcm.administration().getOrCreateCache(CACHE_NAME,
         new StringConfiguration(
            "<distributed-cache>" +
               "<encoding>" +
                  "<key media-type=\"" + MediaType.TEXT_PLAIN_TYPE + "\"/>" +
                  "<value media-type=\"" + MediaType.APPLICATION_JSON_TYPE + "\"/>" +
               "</encoding>" +
            "</distributed-cache>"));
   }

   @Override
   public OffsetBackend offsetBackendForURI(URI uri) {
      RemoteCacheManager remoteCacheManager = managerForURI(uri);
      return new HotRodOffsetBackend(remoteCacheManager.getCache(OFFSET_CACHE_NAME));
   }

   private void getOrCreateOffsetBackendCache(RemoteCacheManager rcm) {
      rcm.administration()
         .getOrCreateCache(OFFSET_CACHE_NAME, new StringConfiguration(
            "<replicated-cache>" +
               "<encoding media-type=\"" + MediaType.APPLICATION_OCTET_STREAM_TYPE + "\"/>" +
            "</replicated-cache>"));
   }

   @Override
   public SchemaBackend schemaBackendForURI(URI uri) {
      RemoteCacheManager remoteCacheManager = managerForURI(uri);
      getOrCreateSchemaBackendCache(remoteCacheManager, SCHEMA_CACHE_NAME);
      MultimapCacheManager<String, String> remoteMultimapCacheManager = RemoteMultimapCacheManagerFactory.from(remoteCacheManager);
      // Support duplicates so it uses a list which is ordered
      return new HotRodSchemaBackend(remoteMultimapCacheManager.get(SCHEMA_CACHE_NAME, true));
   }

   private RemoteCache<String, String> getOrCreateSchemaBackendCache(RemoteCacheManager rcm, String multiMapCacheName) {
      return rcm.administration().getOrCreateCache(multiMapCacheName, new StringConfiguration(
            "<replicated-cache>" +
               "<encoding>" +
                  "<key media-type=\"" + MediaType.TEXT_PLAIN_TYPE + "\"/>" +
                  "<value media-type=\"" + MediaType.APPLICATION_OCTET_STREAM_TYPE + "\"/>" +
               "</encoding>" +
            "</replicated-cache>"));
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
         rcm.stopAsync();
      }
   }
}
