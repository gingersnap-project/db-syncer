package io.gingersnapproject.cdc.event;

import java.net.URI;

import io.gingersnapproject.cdc.cache.CacheIdentifier;
import io.gingersnapproject.cdc.connector.DatabaseProvider;

public class Events {

   private Events() { }

   public record BackendFailedEvent(CacheIdentifier identifier, Throwable throwable) { }

   public record BackendStartedEvent(CacheIdentifier identifier, boolean reconnect) { }

   public record BackendStoppedEvent(CacheIdentifier identifier) { }

   public record ConnectorFailedEvent(CacheIdentifier identifier, Throwable throwable) { }

   public record ConnectorStartedEvent(CacheIdentifier identifier, DatabaseProvider databaseProvider) { }

   public record ConnectorStoppedEvent(CacheIdentifier identifier) { }

   public record CacheMemberJoinEvent(URI uri) { }

   public record CacheMemberLeaveEvent(URI uri) { }
}
