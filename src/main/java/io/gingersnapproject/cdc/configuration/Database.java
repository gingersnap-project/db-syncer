package io.gingersnapproject.cdc.configuration;

import java.util.Optional;

import io.gingersnapproject.cdc.connector.DatabaseProvider;

public interface Database {

   static String property(String p) {
      return String.format("gingersnap.database.%s", p);
   }

   DatabaseProvider type();

   String host();

   int port();

   String username();

   String password();

   Optional<String> database();
}
