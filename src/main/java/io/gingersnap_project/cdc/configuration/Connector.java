package io.gingersnap_project.cdc.configuration;

import io.quarkus.runtime.annotations.ConfigGroup;

@ConfigGroup
public interface Connector {

   String connector();

   String schema();

   String table();
}
