package org.gingesnap.cdc.configuration;

import io.quarkus.runtime.annotations.ConfigGroup;

@ConfigGroup
public interface Connector {

   String connector();

   String topic();

   String schema();

   String table();
}
