package org.gingesnap.cdc.cache.hotrod;

import java.net.URI;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.inject.Singleton;

import org.gingesnap.cdc.CacheBackend;
import org.gingesnap.cdc.OffsetBackend;
import org.gingesnap.cdc.SchemaBackend;
import org.gingesnap.cdc.cache.CacheService;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.multimap.MultimapCacheManager;
import org.infinispan.client.hotrod.multimap.RemoteMultimapCacheManagerFactory;
import org.infinispan.commons.configuration.XMLStringConfiguration;
import org.infinispan.commons.dataconversion.MediaType;

import io.quarkus.arc.lookup.LookupIfProperty;

@LookupIfProperty(name = "service.hotrod.enabled", stringValue = "true", lookupIfMissing = true)
@Singleton
public class HotRodService implements CacheService {
   ConcurrentMap<URI, RemoteCacheManager> otherURIs = new ConcurrentHashMap<>();

   @Override
   public CacheBackend backendForURI(URI uri) {
      if (!uri.getScheme().startsWith("hotrod")) {
         return null;
      }
      RemoteCacheManager remoteCacheManager = otherURIs.computeIfAbsent(uri, RemoteCacheManager::new);
      return new HotRodCacheBackend(uri, remoteCacheManager.administration().getOrCreateCache("debezium-cache",
            new XMLStringConfiguration(
                  "<distributed-cache>" +
                     "<encoding>" +
                        "<key media-type=\"" + MediaType.TEXT_PLAIN_TYPE + "\"/>" +
                        "<value media-type=\"" + MediaType.APPLICATION_JSON_TYPE + "\"/>" +
                     "</encoding>" +
                  "</distributed-cache>")));
   }

   @Override
   public OffsetBackend offsetBackendForURI(URI uri) {
      if (!uri.getScheme().startsWith("hotrod")) {
         return null;
      }
      RemoteCacheManager remoteCacheManager = otherURIs.computeIfAbsent(uri, RemoteCacheManager::new);
      return new HotRodOffsetBackend(remoteCacheManager.administration()
            .getOrCreateCache("debezium-offset", new XMLStringConfiguration(
                  "<replicated-cache>" +
                     "<encoding media-type=\"" + MediaType.APPLICATION_OCTET_STREAM_TYPE + "\"/>" +
                  "</replicated-cache>")));
   }

   @Override
   public SchemaBackend schemaBackendForURI(URI uri) {
      if (!uri.getScheme().startsWith("hotrod")) {
         return null;
      }

      String multiMapCacheName = "debezium-schema";
      RemoteCacheManager remoteCacheManager = otherURIs.computeIfAbsent(uri, RemoteCacheManager::new);
      remoteCacheManager.administration().getOrCreateCache(multiMapCacheName, new XMLStringConfiguration(
                  "<replicated-cache>" +
                     "<encoding>" +
                        "<key media-type=\"" + MediaType.TEXT_PLAIN_TYPE + "\"/>" +
                        "<value media-type=\"" + MediaType.APPLICATION_OCTET_STREAM_TYPE + "\"/>" +
                     "</encoding>" +
                  "</replicated-cache>"));
      MultimapCacheManager<String, String> remoteMultimapCacheManager = RemoteMultimapCacheManagerFactory.from(remoteCacheManager);
      // Support duplicates so it uses a list which is ordered
      return new HotRodSchemaBackend(remoteMultimapCacheManager.get(multiMapCacheName, true));
   }

   @Override
   public void shutdown() {
      for (Iterator<RemoteCacheManager> valueIter = otherURIs.values().iterator(); valueIter.hasNext(); ) {
         valueIter.next().stopAsync();
         valueIter.remove();
      }
   }
}
