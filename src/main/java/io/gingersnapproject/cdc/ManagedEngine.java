package io.gingersnapproject.cdc;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.context.Dependent;
import javax.enterprise.event.Observes;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.gingersnapproject.cdc.cache.CacheService;
import io.gingersnapproject.cdc.configuration.Configuration;
import io.gingersnapproject.cdc.configuration.Rule;
import io.gingersnapproject.cdc.event.Events;
import io.gingersnapproject.cdc.event.NotificationManager;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;

@ApplicationScoped
public class ManagedEngine implements DynamicRuleManagement {
   private static final Logger log = LoggerFactory.getLogger(ManagedEngine.class);
   private static final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(r ->
         new Thread(r, "scheduled-engine-error-handler"));
   private final Map<String, StartStopEngine> engines = new ConcurrentHashMap<>();

   @Inject Configuration config;

   @Inject  CacheService cacheService;

   @Inject NotificationManager eventing;

   public void start(@Observes StartupEvent ignore) {
      log.info("Starting service");
      for (Map.Entry<String, Rule> entry : config.rules().entrySet()) {
         addRule(entry.getKey(), entry.getValue());
      }
   }

   void stop(@Observes ShutdownEvent ignore) {
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
      engineError(ev.name());
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

   private static void stopEngine(StartStopEngine engine, String name) {
      try {
         log.info("Stopping engine {}", name);
         engine.stop();
      } catch (IOException e) {
         log.error("Failing stopping engine {}", name, e);
         throw new RuntimeException(e);
      }
   }

   @Override
   public void addRule(String name, Rule rule) {
      StartStopEngine sse = engines.computeIfAbsent(name, ignore ->
            new StartStopEngine(new EngineWrapper(name, config, rule, cacheService, eventing))
      );
      sse.start();
   }

   @Override
   public void removeRule(String name) {
       engines.computeIfPresent(name, (ignore, sse) -> {
          try {
             sse.shutdown();
          } catch (IOException e) {
             log.error("Failed engine {} shutdown", name, e);
             throw new RuntimeException(e);
          }
          return null;
       });
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
            case RUNNING, SHUTDOWN -> throw new IllegalArgumentException(
                  "Engine " + engine.getName() + " cannot be started, state was " + status);
            case STOPPED, RETRYING, STOPPING -> {
               if (task != null) {
                  task.close();
                  task = null;
               }
               engine.start();
               status = Status.RUNNING;
            }
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
            case STOPPING, RUNNING -> {
               engine.stop();
               status = Status.STOPPED;
            }
            case RETRYING -> {
               task.close();
               task = null;
               status = Status.STOPPED;
            }
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
