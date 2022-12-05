package io.gingersnapproject.cdc.configuration;

import java.net.URI;

public interface Cache {

   static String property(String p) {
      return String.format("gingersnap.cache.%s", p);
   }

   URI uri();
}
