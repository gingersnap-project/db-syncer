package org.gingesnap.cdc;

import java.io.IOException;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;

import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import org.gingesnap.cdc.configuration.Connector;
import org.gingesnap.cdc.configuration.Database;
import org.jboss.logging.Logger;

@ApplicationScoped
public class ManagedEngine {
   private static final Logger log = Logger.getLogger(ManagedEngine.class);

   @Inject Connector connector;
   @Inject Database database;
   private EngineWrapper engine;

   public void start(@Observes StartupEvent ignore) {
      log.info("Starting engine");
      engine = new EngineWrapper(connector, database);
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
