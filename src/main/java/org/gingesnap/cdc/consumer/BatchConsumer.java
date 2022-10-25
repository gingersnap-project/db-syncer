package org.gingesnap.cdc.consumer;

import java.util.List;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.commons.dataconversion.internal.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;

public class BatchConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<String, String>> {
   private static final Logger log = LoggerFactory.getLogger(BatchConsumer.class);
   private final RemoteCache<String, String> cache;

   public BatchConsumer(RemoteCache<String, String> cache) {
      this.cache = cache;
   }

   @Override
   public void handleBatch(List<ChangeEvent<String, String>> batch, DebeziumEngine.RecordCommitter<ChangeEvent<String, String>> recordCommitter) throws InterruptedException {
      log.info("Processing {} entries", batch.size());

      try {
         for (ChangeEvent<String, String> ev : batch) {
            process(ev);
            recordCommitter.markProcessed(ev);
         }
      } finally {
         recordCommitter.markBatchFinished();
      }
   }

   private void process(ChangeEvent<String, String> event) {
      log.info("Received event...");
      log.info("KEY -> {}", event.key());
      log.info("VALUE -> {}", event.value());

      Json jsonObject = Json.read(event.value());
      Json jsonPayload = jsonObject.at("payload");

      Json jsonBefore = jsonPayload.at("before");
      Json jsonAfter = jsonPayload.at("after");

      log.info("BEFORE -> {}", jsonBefore);
      log.info("AFTER -> {}", jsonAfter);
      switch (jsonPayload.at("op").asString()) {
         case "c":
         case "u":
            cache.put(jsonAfter.at("id").toString(), jsonAfter.toString());
            break;
         case "d":
            cache.remove(jsonBefore.at("id").toString());
            break;
         default:
            log.info("Unrecognized operation [{}] for {}", jsonPayload.at("op"), jsonPayload);
      }
   }
}
