package org.gingesnap.cdc.cache.hotrod;

import java.net.URI;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.gingesnap.cdc.CacheBackend;
import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;

public class HotRodCacheBackend implements CacheBackend {
   final URI uriUsed;
   final RemoteCache<String, String> remoteCache;

   public HotRodCacheBackend(URI uri, RemoteCache<String, String> remoteCache) {
      this.uriUsed = uri;
      this.remoteCache = remoteCache.withDataFormat(DataFormat.builder()
            .keyType(MediaType.TEXT_PLAIN).valueType(MediaType.TEXT_PLAIN).build());
   }

   @Override
   public CompletionStage<Void> remove(String key) {
      return remoteCache.removeAsync(key)
            .thenApply(__ -> null);
   }

   @Override
   public CompletionStage<Void> put(String key, Json json) {
      return remoteCache.putAsync(key, json.toString())
            .thenApply(__ -> null);
   }

   @Override
   public URI uriUsed() {
      return uriUsed;
   }
}
