package io.gingersnapproject.cdc.cache.hotrod;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;

import io.gingersnapproject.cdc.cache.CacheIdentifier;
import io.gingersnapproject.metrics.CacheServiceAccessRecord;
import io.gingersnapproject.metrics.DBSyncerMetrics;
import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;

import io.gingersnapproject.cdc.CacheBackend;
import io.gingersnapproject.cdc.event.NotificationManager;
import io.gingersnapproject.cdc.translation.JsonTranslator;

public class HotRodCacheBackend implements CacheBackend {
   private final CacheIdentifier identifier;
   final RemoteCache<String, String> remoteCache;
   final JsonTranslator<?> keyTranslator;
   final JsonTranslator<?> valueTranslator;
   private final NotificationManager eventing;
   private final BiConsumer<Object, Throwable> eventBiConsumer;
   private final DBSyncerMetrics metrics;
   private boolean stopped;

   public HotRodCacheBackend(CacheIdentifier identifier, RemoteCache<String, String> remoteCache, JsonTranslator<?> keyTranslator,
                             JsonTranslator<?> valueTranslator, NotificationManager eventing, DBSyncerMetrics metrics) {
      this.identifier = identifier;
      this.remoteCache = remoteCache.withDataFormat(DataFormat.builder()
            .keyType(MediaType.TEXT_PLAIN).valueType(MediaType.TEXT_PLAIN).build());
      this.keyTranslator = keyTranslator;
      this.valueTranslator = valueTranslator;
      this.eventing = eventing;
      this.eventBiConsumer = (ignore, t) -> {
         if (t != null) {
            this.eventing.backendFailedEvent(identifier, t);
         }
      };
      this.metrics = metrics;
   }

   HotRodCacheBackend copy(RemoteCache<String, String> rc) {
      var o = new HotRodCacheBackend(identifier, rc, keyTranslator, valueTranslator, eventing, metrics);
      o.stopped = this.stopped;
      return o;
   }

   @Override
   public CompletionStage<Void> remove(Json json) {
      CacheServiceAccessRecord<String> record = metrics.recordCacheServiceRemove();
      try {
         return remoteCache.removeAsync(keyTranslator.apply(json).toString())
               .whenComplete(eventBiConsumer)
               .whenComplete(record)
               .thenApply(__ -> null);
      } catch (Throwable t) {
         record.recordThrowable(t);
         return CompletableFuture.failedFuture(t);
      }
   }

   @Override
   public CompletionStage<Void> put(Json json) {
      CacheServiceAccessRecord<String> record = metrics.recordCacheServicePut();
      try {
      return remoteCache.putAsync(keyTranslator.apply(json).toString(), valueTranslator.apply(json).toString())
            .whenComplete(eventBiConsumer)
            .whenComplete(record)
            .thenApply(__ -> null);
      } catch (RuntimeException t) {
         record.recordThrowable(t);
         return CompletableFuture.failedFuture(t);
      }
   }

   @Override
   public void stop() {
      remoteCache.stop();
      eventing.backendStoppedEvent(identifier);
      stopped = true;
   }

   @Override
   public void start() {
      remoteCache.start();
      eventing.backendStartedEvent(identifier, stopped);
      stopped = false;
   }

   @Override
   public boolean reconnect() {
      if (!stopped) return true;

      try {
         start();
         return isRunning();
      } catch (Throwable t) {
         stopped = true;
         eventing.backendFailedEvent(identifier, t);
      }
      return false;
   }

   @Override
   public boolean isRunning() {
      return !stopped;
   }
}
