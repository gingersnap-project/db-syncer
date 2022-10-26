package org.gingesnap.cdc.configuration;

import java.util.Map;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithName;

@ConfigMapping(prefix = "gingersnap")
public interface Region {

   @WithName("region")
   Map<String, SingleRegion> regions();

   interface SingleRegion {

      Connector connector();

      Database database();

      Backend backend();
   }
}
