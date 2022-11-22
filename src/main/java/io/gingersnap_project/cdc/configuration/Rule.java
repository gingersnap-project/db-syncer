package io.gingersnap_project.cdc.configuration;

import java.util.Map;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithName;

@ConfigMapping(prefix = "gingersnap")
public interface Rule {

   @WithName("rule")
   Map<String, SingleRule> rules();

   interface SingleRule {

      Connector connector();

      Database database();

      Backend backend();
   }
}
