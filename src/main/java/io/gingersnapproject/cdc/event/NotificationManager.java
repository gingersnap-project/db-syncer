package io.gingersnapproject.cdc.event;

import java.net.URI;

import io.gingersnapproject.cdc.cache.CacheIdentifier;
import io.gingersnapproject.cdc.configuration.Database;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Event;
import javax.inject.Inject;

/**
 * The global event manager.
 * <p>Responsible for publishing events for stages of the engines for the different CDI {@link Event}.
 * This approach is based on the <a href="https://github.com/debezium/debezium/blob/d42b5480a3965ca1e7761a5cc7a39af7c890ee2d/debezium-server/debezium-server-core/src/main/java/io/debezium/server/ConnectorLifecycle.java">Debezium Server</a>.</p>
 *
 * <p>Adopting this approach, multiple listeners with different goals can receive the events without
 * changing the internals for specific reasons.</p>
 *
 * @author Jose Bolina
 */
@ApplicationScoped
public class NotificationManager {

   @Inject Event<Events.ConnectorStartedEvent> connectorStartedEvent;
   @Inject Event<Events.ConnectorFailedEvent> connectorFailedEvent;
   @Inject Event<Events.ConnectorStoppedEvent> connectorStoppedEvent;

   @Inject Event<Events.BackendStartedEvent> backendStartedEvent;
   @Inject Event<Events.BackendFailedEvent> backendFailedEvent;
   @Inject Event<Events.BackendStoppedEvent> backendStoppedEvent;

   @Inject Event<Events.CacheMemberJoinEvent> memberJoinEvent;
   @Inject Event<Events.CacheMemberLeaveEvent> memberLeaveEvent;

   public void connectorFailed(CacheIdentifier identifier, Throwable t) {
      connectorFailedEvent.fire(new Events.ConnectorFailedEvent(identifier, t));
   }

   public void connectorStarted(CacheIdentifier identifier, Database provider) {
      connectorStartedEvent.fire(new Events.ConnectorStartedEvent(identifier, provider));
   }

   public void connectorStopped(CacheIdentifier identifier) {
      connectorStoppedEvent.fire(new Events.ConnectorStoppedEvent(identifier));
   }

   public void backendStartedEvent(CacheIdentifier identifier, boolean reconnect) {
      backendStartedEvent.fire(new Events.BackendStartedEvent(identifier, reconnect));
   }

   public void backendFailedEvent(CacheIdentifier identifier, Throwable throwable) {
      backendFailedEvent.fire(new Events.BackendFailedEvent(identifier, throwable));
   }

   public void backendStoppedEvent(CacheIdentifier identifier) {
      backendStoppedEvent.fire(new Events.BackendStoppedEvent(identifier));
   }

   public void memberJoinEvent(URI uri) {
      memberJoinEvent.fire(new Events.CacheMemberJoinEvent(uri));
   }

   public void memberLeaveEvent(URI uri) {
      memberLeaveEvent.fire(new Events.CacheMemberLeaveEvent(uri));
   }
}
