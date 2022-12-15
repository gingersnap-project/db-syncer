package io.gingersnapproject.cdc.configuration;

import java.util.List;
import java.util.Optional;

import io.gingersnapproject.proto.api.config.v1alpha1.KeyFormat;
import io.smallrye.config.WithDefault;

public interface Rule {

      Connector connector();

      @WithDefault("TEXT")
      KeyFormat keyType();

      @WithDefault("|")
      String plainSeparator();

      List<String> keyColumns();

      Optional<List<String>> valueColumns();

}
