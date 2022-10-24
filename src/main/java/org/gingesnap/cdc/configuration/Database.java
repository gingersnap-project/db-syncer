package org.gingesnap.cdc.configuration;

import io.smallrye.config.ConfigMapping;

@ConfigMapping(prefix = "database")
public interface Database {

   String hostname();

   int port();

   String user();

   String password();
}
