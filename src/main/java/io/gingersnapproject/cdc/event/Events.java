package io.gingersnapproject.cdc.event;

import io.gingersnapproject.cdc.connector.DatabaseProvider;

public class Events {

   private Events() { }

   public record BackendFailedEvent(String name, Throwable throwable) { }

   public record BackendStartedEvent(String name, boolean reconnect) { }

   public record BackendStoppedEvent(String name) { }

   public record ConnectorFailedEvent(String name, Throwable throwable) { }

   public record ConnectorStartedEvent(String name, DatabaseProvider databaseProvider) { }

   public record ConnectorStoppedEvent(String name) { }
}
