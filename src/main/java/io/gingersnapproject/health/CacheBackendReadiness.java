package io.gingersnapproject.health;

import java.net.URI;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;

import io.gingersnapproject.cdc.event.Events;

import org.eclipse.microprofile.health.Readiness;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Readiness
@ApplicationScoped
public class CacheBackendReadiness extends AbstractHealthChecker<URI> {
   private static final Logger log = LoggerFactory.getLogger(CacheBackendReadiness.class);
   private static final String BACKEND_HEALTH = "Backend Health";

   void backendStarted(@Observes Events.BackendStartedEvent ev) {
      log.info("Backend {} started", ev.uri());
      isUp(ev.uri());
   }

   void backendStopped(@Observes Events.BackendStoppedEvent ev) {
      log.info("Backend {} stopped", ev.uri());
      isDone(ev.uri());
   }

   void backendFailed(@Observes Events.BackendFailedEvent ev) {
      log.error("Backend {} failed", ev.uri(), ev.throwable());
      isDown(ev.uri());
   }

   @Override
   protected String name() {
      return BACKEND_HEALTH;
   }

   @Override
   public String apply(URI uri) {
      return uri.toString();
   }
}
