package org.gingesnap.cdc.cache.hotrod;

import java.net.URI;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.inject.Singleton;

import org.gingesnap.cdc.CacheBackend;
import org.gingesnap.cdc.OffsetBackend;
import org.gingesnap.cdc.cache.CacheService;
import org.infinispan.client.hotrod.RemoteCacheManager;
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
      return new HotRodOffsetBackend(remoteCacheManager.administration().getOrCreateCache("debezium-offset",
            new XMLStringConfiguration(
                  "<distributed-cache>" +
                        "<encoding media-type=\"" + MediaType.APPLICATION_OCTET_STREAM_TYPE + "\"/>" +
                        "</distributed-cache>")));
   }

   @Override
   public void shutdown() {
      for (Iterator<RemoteCacheManager> valueIter = otherURIs.values().iterator(); valueIter.hasNext(); ) {
         valueIter.next().stopAsync();
         valueIter.remove();
      }
   }
}
