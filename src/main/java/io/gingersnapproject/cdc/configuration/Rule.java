package io.gingersnapproject.cdc.configuration;

public interface Rule {

      Connector connector();

      Backend backend();
}
