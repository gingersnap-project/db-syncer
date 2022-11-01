package org.gingesnap.cdc.consumer;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.gingesnap.cdc.CacheBackend;
import org.gingesnap.cdc.EngineWrapper;
import org.gingesnap.cdc.util.CompletionStages;
import org.infinispan.commons.dataconversion.internal.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;

public class BatchConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<String, String>> {
   private static final Logger log = LoggerFactory.getLogger(BatchConsumer.class);
   private final CacheBackend cache;
   private final EngineWrapper engine;

   public BatchConsumer(CacheBackend cache, EngineWrapper engine) {
      this.cache = cache;
      this.engine = engine;
   }

   @Override
   public void handleBatch(List<ChangeEvent<String, String>> batch, DebeziumEngine.RecordCommitter<ChangeEvent<String, String>> recordCommitter) throws InterruptedException {
      log.info("Processing {} entries", batch.size());

      try {
         for (ChangeEvent<String, String> ev : batch) {
            // TODO: we may be able to do this in parallel later or asynchronously even
            CompletionStages.join(process(ev));
            recordCommitter.markProcessed(ev);
         }
         recordCommitter.markBatchFinished();
      } catch (Throwable t) {
         log.info("Exception encountered writing updates for engine {}", engine.getName(), t);
         engine.notifyError();
      }
   }

   private CompletionStage<Void> process(ChangeEvent<String, String> event) {
      log.info("Received event...");
      log.info("KEY -> {}", event.key());
      log.info("VALUE -> {}", event.value());

      Json jsonObject = Json.read(event.value());
      Json jsonPayload = jsonObject.at("payload");

      Json jsonBefore = jsonPayload.at("before");
      Json jsonAfter = jsonPayload.at("after");

      log.info("BEFORE -> {}", jsonBefore);
      log.info("AFTER -> {}", jsonAfter);
      String op = jsonPayload.at("op").asString();
      switch (op) {
         //create
         case "c":
         // update
         case "u":
         // snapshot
         case "r":
            return cache.put(jsonAfter);
         //delete
         case "d":
            return cache.remove(jsonBefore);
         default:
            log.info("Unrecognized operation [{}] for {}", jsonPayload.at("op"), jsonPayload);
            return CompletableFuture.completedFuture(null);
      }
   }
}
