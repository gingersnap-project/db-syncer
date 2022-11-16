package io.gingersnap_project.cdc.chain;

import io.gingersnap_project.cdc.CacheBackend;
import io.gingersnap_project.cdc.configuration.Region;

public class EventProcessingChainFactory {

   public static EventProcessingChain create(Region.SingleRegion region, CacheBackend backend) {
      return EventProcessingChain.chained(
            // The filter link should always be the head.
            new EventFilterLink(region),

            // The cache link should always be last.
            new CacheBackendLink(backend)
      );
   }
}
