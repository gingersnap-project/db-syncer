package io.gingersnapproject.cdc.configuration;

import java.util.Map;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;

@ConfigMapping(prefix = "gingersnap")
public interface Configuration {

   @WithDefault("false")
   boolean dynamicMembership();

   Database database();

   Cache cache();

   @WithName("rule")
   Map<String, Rule> rules();
}
