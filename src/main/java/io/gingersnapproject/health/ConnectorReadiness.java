package io.gingersnapproject.health;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;

import io.gingersnapproject.cdc.event.Events;

import org.eclipse.microprofile.health.Readiness;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Readiness
@ApplicationScoped
public class ConnectorReadiness extends AbstractHealthChecker<String> {
   private static final Logger log = LoggerFactory.getLogger(ConnectorReadiness.class);
   private static final String CONNECTOR_HEALTH = "Connector health";

   void engineStarted(@Observes Events.ConnectorStartedEvent ev) {
      log.info("Engine {} started", ev.name());
      isUp(ev.name());
   }

   void engineStopped(@Observes Events.ConnectorStoppedEvent ev) {
      log.info("Engine {} stopped", ev.name());
      isDone(ev.name());
   }

   void engineFailed(@Observes Events.ConnectorFailedEvent ev) {
      log.error("Engine {} failed", ev.name(), ev.throwable());
      isDown(ev.name());
   }

   @Override
   protected String name() {
      return CONNECTOR_HEALTH;
   }

   @Override
   public String apply(String s) {
      return s;
   }
}
