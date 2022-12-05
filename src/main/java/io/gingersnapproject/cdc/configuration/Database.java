package io.gingersnapproject.cdc.configuration;

import java.util.Optional;

import io.gingersnapproject.cdc.connector.DatabaseProvider;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithName;
import io.smallrye.config.WithParentName;

public interface Database {

   static String property(String p) {
      return String.format("gingersnap.database.%s", p);
   }

   DatabaseProvider type();

   String host();

   int port();

   String user();

   String password();

   Optional<String> database();
}
