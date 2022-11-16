package io.gingersnap_project.cdc.chain;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import io.gingersnap_project.cdc.CacheBackend;
import io.gingersnap_project.cdc.util.CompletionStages;

import org.infinispan.commons.dataconversion.internal.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Link responsible for applying the {@link Event} to the {@link CacheBackend}.
 * <p><b>This link should always be the last in the chain</b>, allowing aborting execution in previous links.</p>
 */
public class CacheBackendLink extends EventProcessingChain {
   private static final Logger log = LoggerFactory.getLogger(CacheBackendLink.class);

   private final CacheBackend cache;

   public CacheBackendLink(CacheBackend cache) {
      this.cache = cache;
   }

   @Override
   public boolean process(Event event, EventContext ctx) {
      return CompletionStages.join(processAsync(event));
   }

   private CompletionStage<Boolean> processAsync(Event event) {
      Json jsonPayload = event.value();

      Json jsonBefore = jsonPayload.at("before");
      Json jsonAfter = jsonPayload.at("after");

      log.info("BEFORE -> {}", jsonBefore);
      log.info("AFTER -> {}", jsonAfter);
      String op = jsonPayload.at("op").asString();
      switch (op) {
         // create
         case "c":
         // snapshot
         case "r":
         // update
         case "u":
            return cache.put(jsonAfter).thenApply(ignore -> true);
         //delete
         case "d":
            return cache.remove(jsonBefore).thenApply(ignore -> true);
         default:
            log.info("Unrecognized operation [{}] for {}", jsonPayload.at("op"), jsonPayload);
            return CompletableFuture.completedFuture(false);
      }
   }
}
