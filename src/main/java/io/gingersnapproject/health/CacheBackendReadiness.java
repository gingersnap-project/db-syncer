package io.gingersnapproject.health;

import java.net.URI;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;

import org.eclipse.microprofile.health.Readiness;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.gingersnapproject.cdc.configuration.Configuration;
import io.gingersnapproject.cdc.event.Events;

@Readiness
@ApplicationScoped
public class CacheBackendReadiness extends AbstractHealthChecker<URI> {
   private static final Logger log = LoggerFactory.getLogger(CacheBackendReadiness.class);
   private static final String BACKEND_HEALTH = "Backend Health";

   @Inject
   Configuration config;

   void backendStarted(@Observes Events.BackendStartedEvent ev) {
      log.info("Backend {} started", config.cache().uri());
      isUp(config.cache().uri());
   }

   void backendStopped(@Observes Events.BackendStoppedEvent ev) {
      log.info("Backend {} stopped", config.cache().uri());
      isDone(config.cache().uri());
   }

   void backendFailed(@Observes Events.BackendFailedEvent ev) {
      log.error("Backend {} failed", config.cache().uri(), ev.throwable());
      isDown(config.cache().uri());
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
