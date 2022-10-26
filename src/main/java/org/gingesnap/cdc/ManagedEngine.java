package org.gingesnap.cdc;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;

import org.gingesnap.cdc.cache.CacheService;
import org.gingesnap.cdc.configuration.Backend;
import org.gingesnap.cdc.configuration.Region;
import org.jboss.logging.Logger;

import io.quarkus.arc.All;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;

@ApplicationScoped
public class ManagedEngine {
   private static final Logger log = Logger.getLogger(ManagedEngine.class);
   private final Map<String, StartStopEngine> engines = new ConcurrentHashMap<>();

   @Inject Instance<Region> runtimeConfiguration;

   @Inject @All List<CacheService> services;

   CacheBackend findBackend(URI uri) {
      for (CacheService cacheService : services) {
         CacheBackend backend = cacheService.backendForURI(uri);
         if (backend != null) {
            return backend;
         }
      }
      throw new IllegalArgumentException("Unsupported URI received: " + uri + " ensure service is running if correct!");
   }

   private CacheBackend findBackend(Backend configuration) {
      return findBackend(URI.create(configuration.uri()));
   }

   public void start(@Observes StartupEvent ignore) {
      log.info("Starting engine");
      Region configuration = runtimeConfiguration.get();
      for (Map.Entry<String, Region.SingleRegion> entry : configuration.regions().entrySet()) {
         StartStopEngine sse = engines.computeIfAbsent(entry.getKey(), name -> {
            Region.SingleRegion regionConfiguration = entry.getValue();
            CacheBackend backend = findBackend(regionConfiguration.backend());
            EngineWrapper engine = new EngineWrapper(regionConfiguration.connector(), regionConfiguration.database(), backend);
            return new StartStopEngine(engine);
         });
         sse.start();
      }
   }

   public void stop(@Observes ShutdownEvent ignore) {
      log.info("Stopping engine");

      try {
         for (StartStopEngine engine : engines.values()) {
            engine.shutdown();
         }
      } catch (IOException e) {
         log.error("Failed shutdown engine", e);
      }
   }

   public void start(String name) {
      StartStopEngine sse = engines.get(name);
      if (sse != null) {
         sse.start();
      }
   }

   public void stop(String name) {
      StartStopEngine sse = engines.get(name);
      if (sse != null) {
         try {
            sse.stop();
         } catch (IOException e) {
            log.error("Failing stopping engine {}", name, e);
            throw new RuntimeException(e);
         }
      }
   }

   private static class StartStopEngine {
      private final EngineWrapper engine;
      private boolean running;
      private boolean shutdown;

      public StartStopEngine(EngineWrapper engine) {
         this.engine = engine;
         this.running = false;
         this.shutdown = false;
      }

      public void start() {
         if (!running && !shutdown) {
            engine.start();
            running = true;
         }
      }

      public void stop() throws IOException {
         if (running) {
            engine.stop();
            running = false;
         }
      }

      public void shutdown() throws IOException {
         if (running) {
            engine.stop();
         }
         shutdown = true;
         running = false;
      }
   }
}
