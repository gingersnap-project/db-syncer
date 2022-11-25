package io.gingersnapproject.cdc;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;

import io.gingersnapproject.cdc.cache.CacheService;
import io.gingersnapproject.cdc.configuration.Rule;
import io.gingersnapproject.cdc.event.Events;
import io.gingersnapproject.cdc.event.NotificationManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.quarkus.arc.All;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;

@ApplicationScoped
public class ManagedEngine {
   private static final Logger log = LoggerFactory.getLogger(ManagedEngine.class);
   private static final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(r ->
         new Thread(r, "scheduled-engine-error-handler"));
   private final Map<String, StartStopEngine> engines = new ConcurrentHashMap<>();

   @Inject
   Rule runtimeConfiguration;

   @Inject @All List<CacheService> services;
   @Inject NotificationManager eventing;

    CacheService findCacheService(URI uri) {
      for (CacheService cacheService : services) {
         if (cacheService.supportsURI(uri)) {
            return cacheService;
         }
      }
      throw new IllegalArgumentException("Unsupported URI received: " + uri + " ensure service is running if correct!");
   }

   public void start(@Observes StartupEvent ignore) {
      log.info("Starting service");
      for (Map.Entry<String, Rule.SingleRule> entry : runtimeConfiguration.rules().entrySet()) {
         StartStopEngine sse = engines.computeIfAbsent(entry.getKey(), name -> {
            Rule.SingleRule ruleConfiguration = entry.getValue();
            URI uri = ruleConfiguration.backend().uri();
            CacheService cacheService = findCacheService(uri);
            EngineWrapper engine = new EngineWrapper(name, ruleConfiguration, cacheService, eventing);
            return new StartStopEngine(engine);
         });
         sse.start();
      }
   }

   public void stop(@Observes ShutdownEvent ignore) {
      log.info("Service shutting down");

      try {
         for (StartStopEngine engine : engines.values()) {
            engine.shutdown();
         }
      } catch (IOException e) {
         log.error("Failed shutdown engine", e);
      }
   }

   void engineFailed(@Observes Events.ConnectorFailedEvent ev) {
       engineError(ev.name());
   }

   void backendFailed(@Observes Events.BackendFailedEvent ev) {
       URI failedUri = ev.uri();
      for (Map.Entry<String, Rule.SingleRule> entry : runtimeConfiguration.rules().entrySet()) {
         Rule.SingleRule rule = entry.getValue();
         if (rule.backend().uri().equals(failedUri)) {
            engineError(entry.getKey());
            break;
         }
      }
   }

   public StartStopEngine start(String name) {
      StartStopEngine sse = engines.get(name);
      if (sse != null) {
         log.info("Starting engine {}", name);
         sse.start();
      }
      return sse;
   }

   public StartStopEngine stop(String name) {
      StartStopEngine sse = engines.get(name);
      if (sse != null) {
         stopEngine(sse, name);
      }
      return sse;
   }

   private static void stopEngine(StartStopEngine engine, String name) {
      try {
         log.info("Stopping engine {}", name);
         engine.stop();
      } catch (IOException e) {
         log.error("Failing stopping engine {}", name, e);
         throw new RuntimeException(e);
      }
   }

   public void engineError(String name) {
      StartStopEngine sse = engines.get(name);
      // Try to mark stop pending, only first caller should submit task
      if (sse == null || !sse.attemptMarkStopping()) {
         return;
      }
      // Have to submit on a different thread to not block the debezium poll loop
      scheduledExecutorService.submit(((() -> {
            stopEngine(sse, name);
            log.info("Scheduling retry for engine {}", name);
            ReschedulingTask<Boolean> reschedulingTask = new ReschedulingTask<>(scheduledExecutorService, sse.engine::cacheServiceAvailable,
                  available -> {
                     if (available) {
                        sse.start();
                     }
                     return !available;
                  }, 10, TimeUnit.SECONDS,
                  t -> {
                     log.trace("Retry task encountered error for engine {}, rescheduling attempt again later", name, t);
                     return true;
                  });
            sse.markRetrying(reschedulingTask.schedule());
         }
      )));
   }

   private enum Status {
      SHUTDOWN,
      RUNNING,
      STOPPING,
      STOPPED,
      RETRYING;
   }

   private static class StartStopEngine {
      private final EngineWrapper engine;
      private Status status = Status.STOPPED;
      private ReschedulingTask<Boolean> task;

      public StartStopEngine(EngineWrapper engine) {
         this.engine = engine;
      }

      public synchronized void start() {
         switch (status) {
            case RUNNING:
            case SHUTDOWN:
               throw new IllegalArgumentException("Engine " + engine.getName() + " cannot be started, state was " + status);
            case STOPPED:
            case RETRYING:
            case STOPPING:
               if (task != null) {
                  task.close();
                  task = null;
               }
               engine.start();
               status = Status.RUNNING;
         }
      }

      public synchronized boolean attemptMarkStopping() {
         if (status == Status.RUNNING) {
            status = Status.STOPPING;
            return true;
         }
         return false;
      }

      public synchronized void stop() throws IOException {
          switch (status) {
             case STOPPING:
             case RUNNING:
                engine.stop();
                status = Status.STOPPED;
                break;
             case RETRYING:
                task.close();
                task = null;
                status = Status.STOPPED;
                break;
          }
      }

      public synchronized void shutdown() throws IOException {
         switch (status) {
            case SHUTDOWN:
               throw new IllegalArgumentException("Engine " + engine.getName() + " was already shutdown");
            case RUNNING:
               engine.stop();
            default:
               if (task != null) {
                  task.close();
                  task = null;
               }
               engine.shutdownCacheService();
               status = Status.SHUTDOWN;
         }
      }

      public synchronized void markRetrying(ReschedulingTask<Boolean> task) {
         if (status != Status.STOPPED) {
            throw new IllegalArgumentException("Engine " + engine.getName() + " cannot be marked as retrying, state was " + status);
         }
         status = Status.RETRYING;
         // This shouldn't happen, but make sure only a single task running
         if (this.task != null) {
            this.task.close();
         }
         this.task = task;
      }
   }
}
