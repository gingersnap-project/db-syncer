package io.gingersnap_project.cdc.remote;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Future;

import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.kafka.connect.util.Callback;
import io.gingersnap_project.cdc.OffsetBackend;
import io.gingersnap_project.cdc.cache.CacheService;
import io.gingersnap_project.cdc.cache.ErrorNotifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.quarkus.arc.Arc;
import io.quarkus.arc.InstanceHandle;

public class RemoteOffsetStore implements OffsetBackingStore {
   public static final String URI_CACHE = "offset.storage.remote.uri";
   public static final String TOPIC_NAME = "offset.storage.remote.topic";
   private static final Logger log = LoggerFactory.getLogger(RemoteOffsetStore.class);

   private OffsetBackend offsetBackend;
   private String topicName;

   @Override
   public void start() {
      log.info("Starting remote offset store");
   }

   @Override
   public void stop() {
      log.info("Stopping remote offset store.");
   }

   @Override
   public Future<Map<ByteBuffer, ByteBuffer>> get(Collection<ByteBuffer> collection) {
      log.info("Getting {}", collection);
      return offsetBackend.get(collection).whenComplete((v, t) -> {
         if (t != null) {
            ErrorNotifier.notifyError(topicName);
         }
      }).toCompletableFuture();
   }

   @Override
   public Future<Void> set(Map<ByteBuffer, ByteBuffer> map, Callback<Void> callback) {
      log.info("Setting {}", map);
      return offsetBackend.set(map, callback).whenComplete((v, t) -> {
         if (t != null) {
            ErrorNotifier.notifyError(topicName);
         }
      }).toCompletableFuture();
   }

   @Override
   public void configure(WorkerConfig workerConfig) {
      log.info("Configuring offset store {}", workerConfig);
      topicName = (String) workerConfig.originals().get(TOPIC_NAME);
      String stringURI = (String) workerConfig.originals().get(URI_CACHE);
      URI uri = URI.create(stringURI);
      for (InstanceHandle<CacheService> instanceHandle : Arc.container().listAll(CacheService.class)) {
         CacheService cacheService = instanceHandle.get();
         if (cacheService.supportsURI(uri)) {
            offsetBackend = cacheService.offsetBackendForURI(uri);
            break;
         }
      }
      if (offsetBackend == null) {
         throw new IllegalStateException("No offset cache storage for uri: " + uri);
      }
   }
}
