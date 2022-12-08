package io.gingersnapproject.cdc.configuration;

import java.util.List;
import java.util.Optional;

import io.smallrye.config.WithDefault;

public interface Rule {

      Connector connector();

      @WithDefault("PLAIN")
      KeyType keyType();

      @WithDefault("|")
      String plainSeparator();

      List<String> keyColumns();

      Optional<List<String>> columns();

      enum KeyType {
            PLAIN,
            JSON
      }
}
