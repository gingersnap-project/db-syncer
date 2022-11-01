package org.gingesnap.cdc.configuration;

import java.net.URI;
import java.util.List;
import java.util.Optional;

import io.smallrye.config.WithDefault;

public interface Backend {

   URI uri();

   @WithDefault("PLAIN")
   KeyType keyType();

   @WithDefault("true")
   boolean prefixRuleName();

   @WithDefault("|")
   String plainSeparator();

   @WithDefault("rule")
   String jsonRuleName();

   Optional<List<String>> keyColumns();

   Optional<List<String>> columns();

   enum KeyType {
      PLAIN,
      JSON;
   }
}
