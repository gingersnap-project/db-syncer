package io.gingersnapproject.cdc.cache.hotrod;

import java.net.URI;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;

import io.gingersnapproject.cdc.CacheBackend;
import io.gingersnapproject.cdc.event.NotificationManager;
import io.gingersnapproject.cdc.translation.JsonTranslator;
import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;

public class HotRodCacheBackend implements CacheBackend {
   final URI uriUsed;
   final RemoteCache<String, String> remoteCache;
   final JsonTranslator<?> keyTranslator;
   final JsonTranslator<?> valueTranslator;
   private final NotificationManager eventing;
   private final BiConsumer<Object, Throwable> eventBiConsumer;

   public HotRodCacheBackend(URI uri, RemoteCache<String, String> remoteCache,
                             JsonTranslator<?> keyTranslator, JsonTranslator<?> valueTranslator,
                             NotificationManager eventing) {
      this.uriUsed = uri;
      this.remoteCache = remoteCache.withDataFormat(DataFormat.builder()
            .keyType(MediaType.TEXT_PLAIN).valueType(MediaType.TEXT_PLAIN).build());
      this.keyTranslator = keyTranslator;
      this.valueTranslator = valueTranslator;
      this.eventing = eventing;
      this.eventBiConsumer = (ignore, t) -> {
         if (t != null) {
            this.eventing.backendFailedEvent(uriUsed, t);
         }
      };
   }

   @Override
   public CompletionStage<Void> remove(Json json) {
      return remoteCache.removeAsync(keyTranslator.apply(json).toString())
            .whenComplete(eventBiConsumer)
            .thenApply(__ -> null);
   }

   @Override
   public CompletionStage<Void> put(Json json) {
      return remoteCache.putAsync(keyTranslator.apply(json).toString(), valueTranslator.apply(json).toString())
            .whenComplete(eventBiConsumer)
            .thenApply(__ -> null);
   }

   @Override
   public URI uriUsed() {
      return uriUsed;
   }
}
