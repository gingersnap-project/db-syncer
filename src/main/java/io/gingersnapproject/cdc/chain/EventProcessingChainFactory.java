package io.gingersnapproject.cdc.chain;

import io.gingersnapproject.cdc.CacheBackend;
import io.gingersnapproject.cdc.configuration.Rule;

public class EventProcessingChainFactory {

   public static EventProcessingChain create(Rule rule, CacheBackend backend) {
      return EventProcessingChain.chained(
            // The filter link should always be the head.
            new EventFilterLink(rule),

            // The cache link should always be last.
            new CacheBackendLink(backend)
      );
   }
}
