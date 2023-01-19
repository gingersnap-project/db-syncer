package io.gingersnapproject.cdc.event;

import java.net.URI;

import io.gingersnapproject.cdc.connector.DatabaseProvider;

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

   public void connectorFailed(String name, Throwable t) {
      connectorFailedEvent.fire(new Events.ConnectorFailedEvent(name, t));
   }

   public void connectorStarted(String name, DatabaseProvider provider) {
      connectorStartedEvent.fire(new Events.ConnectorStartedEvent(name, provider));
   }

   public void connectorStopped(String name) {
      connectorStoppedEvent.fire(new Events.ConnectorStoppedEvent(name));
   }

   public void backendStartedEvent(String name, boolean reconnect) {
      backendStartedEvent.fire(new Events.BackendStartedEvent(name, reconnect));
   }

   public void backendFailedEvent(String name, Throwable throwable) {
      backendFailedEvent.fire(new Events.BackendFailedEvent(name, throwable));
   }

   public void backendStoppedEvent(String name) {
      backendStoppedEvent.fire(new Events.BackendStoppedEvent(name));
   }

   public void memberJoinEvent(URI uri) {
      memberJoinEvent.fire(new Events.CacheMemberJoinEvent(uri));
   }

   public void memberLeaveEvent(URI uri) {
      memberLeaveEvent.fire(new Events.CacheMemberLeaveEvent(uri));
   }
}
