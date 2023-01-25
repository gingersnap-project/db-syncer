package io.gingersnapproject.cdc.cache;

import java.net.URI;

public record CacheIdentifier(String rule, URI uri) {

   public static CacheIdentifier of(String rule, URI uri) {
      return new CacheIdentifier(rule, uri);
   }

   @Override
   public String toString() {
      return String.format("%s-%s", rule, uri.getHost());
   }
}
