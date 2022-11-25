package io.gingersnapproject.cdc.event;

import java.net.URI;

public class Events {

   private Events() { }

   public record BackendFailedEvent(URI uri, Throwable throwable) { }

   public record BackendStartedEvent(URI uri) { }

   public record BackendStoppedEvent(URI uri) { }

   public record ConnectorFailedEvent(String name, Throwable throwable) { }

   public record ConnectorStartedEvent(String name) { }

   public record ConnectorStoppedEvent(String name) { }
}
