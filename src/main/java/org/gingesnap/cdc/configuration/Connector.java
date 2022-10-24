package org.gingesnap.cdc.configuration;

import io.smallrye.config.ConfigMapping;

@ConfigMapping(prefix = "connector")
public interface Connector {

   String connector();

   String topic();

   String schema();

   String table();
}
