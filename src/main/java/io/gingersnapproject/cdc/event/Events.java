package io.gingersnapproject.cdc.event;

public class Events {

   private Events() { }

   public record BackendFailedEvent(Throwable throwable) { }

   public record BackendStartedEvent() { }

   public record BackendStoppedEvent() { }

   public record ConnectorFailedEvent(String name, Throwable throwable) { }

   public record ConnectorStartedEvent(String name) { }

   public record ConnectorStoppedEvent(String name) { }
}
