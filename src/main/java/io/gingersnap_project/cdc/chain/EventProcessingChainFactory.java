package io.gingersnap_project.cdc.chain;

import io.gingersnap_project.cdc.CacheBackend;
import io.gingersnap_project.cdc.configuration.Rule;

public class EventProcessingChainFactory {

   public static EventProcessingChain create(Rule.SingleRule rule, CacheBackend backend) {
      return EventProcessingChain.chained(
            // The filter link should always be the head.
            new EventFilterLink(rule),

            // The cache link should always be last.
            new CacheBackendLink(backend)
      );
   }
}
