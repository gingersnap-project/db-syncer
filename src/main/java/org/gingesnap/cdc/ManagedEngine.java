package org.gingesnap.cdc;

import java.io.IOException;
import java.net.URI;
import java.util.List;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;

import org.gingesnap.cdc.cache.CacheService;
import org.gingesnap.cdc.configuration.Connector;
import org.gingesnap.cdc.configuration.Database;
import org.jboss.logging.Logger;

import io.quarkus.arc.All;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;

@ApplicationScoped
public class ManagedEngine {
   private static final Logger log = Logger.getLogger(ManagedEngine.class);

   @Inject Connector connector;
   @Inject Database database;
   @Inject @All List<CacheService> services;
   private EngineWrapper engine;

   CacheBackend findBackend(URI uri) {
      for (CacheService cacheService : services) {
         CacheBackend backend = cacheService.backendForURI(uri);
         if (backend != null) {
            return backend;
         }
      }
      throw new IllegalArgumentException("Unsupported URI received: " + uri + " ensure service is running if correct!");
   }

   public void start(@Observes StartupEvent ignore) {
      log.info("Starting engine");
      engine = new EngineWrapper(connector, database, findBackend(URI.create("hotrod://admin:password@127.0.0.1:11222?sasl_mechanism=SCRAM-SHA-512")));
      engine.start();
   }

   public void stop(@Observes ShutdownEvent ignore) {
      log.info("Stopping engine");

      try {
         engine.stop();
      } catch (IOException e) {
         log.error("Failed shutdown engine", e);
      }
   }
}
