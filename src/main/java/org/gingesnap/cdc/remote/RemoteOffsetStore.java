package org.gingesnap.cdc.remote;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.kafka.connect.util.Callback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RemoteOffsetStore implements OffsetBackingStore {
   private static final Logger log = LoggerFactory.getLogger(RemoteOffsetStore.class);
   private final ExecutorService executor = Executors.newSingleThreadExecutor(new ThreadFactory() {
      @Override
      public Thread newThread(Runnable runnable) {
         return new Thread(runnable, "remote-offset");
      }
   });


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
      return executor.submit(() -> Collections.emptyMap());
   }

   @Override
   public Future<Void> set(Map<ByteBuffer, ByteBuffer> map, Callback<Void> callback) {
      log.info("Setting {}", map);
      return executor.submit(() -> null);
   }

   @Override
   public void configure(WorkerConfig workerConfig) {
      log.info("Configuring offset store");
   }
}
