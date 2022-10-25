package org.gingesnap.cdc;

import java.io.IOException;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;

import org.gingesnap.cdc.configuration.Connector;
import org.gingesnap.cdc.configuration.Database;
import org.infinispan.client.hotrod.RemoteCache;
import org.jboss.logging.Logger;

import io.quarkus.infinispan.client.Remote;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;

@ApplicationScoped
public class ManagedEngine {
   private static final Logger log = Logger.getLogger(ManagedEngine.class);

   @Inject Connector connector;
   @Inject Database database;
   @Inject @Remote("debezium-cache")
   RemoteCache<String, String> cache;
   private EngineWrapper engine;

   public void start(@Observes StartupEvent ignore) {
      log.info("Starting engine");
      engine = new EngineWrapper(connector, database, cache);
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
