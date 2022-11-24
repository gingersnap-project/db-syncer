package io.gingersnapproject.cdc.configuration;

import java.util.Optional;

import io.quarkus.runtime.annotations.ConfigGroup;

@ConfigGroup
public interface Database {

   String hostname();

   int port();

   String user();

   String password();

   Optional<String> database();
}
