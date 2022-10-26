package org.gingesnap.cdc.configuration;

import io.quarkus.runtime.annotations.ConfigGroup;

@ConfigGroup
public interface Database {

   String hostname();

   int port();

   String user();

   String password();
}
