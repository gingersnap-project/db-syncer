package io.gingersnapproject.cdc;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;

import io.gingersnapproject.cdc.cache.CacheIdentifier;
import io.gingersnapproject.cdc.cache.CacheService;
import io.gingersnapproject.cdc.configuration.Configuration;
import io.gingersnapproject.cdc.configuration.Rule;
import io.gingersnapproject.cdc.event.Events;
import io.gingersnapproject.cdc.event.NotificationManager;
import io.gingersnapproject.cdc.util.AggregateCompletionStage;
import io.gingersnapproject.cdc.util.CompletionStages;

import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class ManagedEngine implements DynamicRuleManagement {
   private static final Logger log = LoggerFactory.getLogger(ManagedEngine.class);
   private static final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(r ->
         new Thread(r, "scheduled-engine-error-handler"));
   private final Map<CacheIdentifier, StartStopEngine> engines = new ConcurrentHashMap<>();
   private final Map<String, Rule> knownRules = new ConcurrentHashMap<>();
   private final Set<URI> knownMembers = ConcurrentHashMap.newKeySet();

   @Inject Configuration config;

   @Inject CacheService cacheService;

   @Inject NotificationManager eventing;

   public void start(@Observes StartupEvent ignore) {
      log.info("Starting service");
      knownRules.putAll(config.rules());
      addRuleWithKnownMembers();
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

   void memberJoined(@Observes Events.CacheMemberJoinEvent ev) {
      log.info("New member joined {}", ev);
      if (knownMembers.add(ev.uri())) addRuleWithKnownMembers();
   }

   void memberLeft(@Observes Events.CacheMemberLeaveEvent ev) {
      log.info("Member left {}", ev);
      if (!knownMembers.remove(ev.uri())) return;
      remove(identifier ->  identifier.uri().equals(ev.uri()));
   }

   void engineFailed(@Observes Events.ConnectorFailedEvent ev) {
       engineError(ev.identifier());
   }

   void backendFailed(@Observes Events.BackendFailedEvent ev) {
      engineError(ev.identifier());
   }

   private void engineError(CacheIdentifier identifier) {
      StartStopEngine sse = engines.get(identifier);
      // Try to mark stop pending, only first caller should submit task
      if (sse == null || !sse.attemptMarkStopping()) {
         return;
      }
      // Have to submit on a different thread to not block the debezium poll loop
      scheduledExecutorService.submit(() -> {
         stopEngine(sse, sse.engine.getName());
         log.info("Scheduling retry for engine {}", identifier);

         // Executor has a single thread, do not join the completable.
         retryEngineStart(identifier, sse);
      });
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

   private static void shutdownEngine(StartStopEngine engine, String name) {
      try {
         engine.shutdown();
      } catch (IOException e) {
         log.error("Failed engine {} shutdown", name, e);
         throw new RuntimeException(e);
      }
   }

   @Override
   public void addRule(String name, Rule rule) {
      knownRules.put(name, rule);
      addRuleWithKnownMembers();
   }

   private void addRuleWithKnownMembers() {
      AggregateCompletionStage<Void> stage = CompletionStages.aggregateCompletionStage();
      for (Map.Entry<String, Rule> rule : knownRules.entrySet()) {
         for (URI memberURI : knownMembers) {
            var identifier = CacheIdentifier.of(rule.getKey(), memberURI);
            engines.computeIfAbsent(identifier, ignore -> {
               log.info("Creating new engine for {}", identifier);
               var sse = new StartStopEngine(new EngineWrapper(identifier, config, rule.getValue(), cacheService, eventing));
               stage.dependsOn(startEngine(identifier, sse));
               return sse;
            });
         }
      }
   }

   private CompletionStage<Void> startEngine(CacheIdentifier identifier, StartStopEngine sse) {
      try {
         sse.start();
         return CompletableFutures.completedNull();
      } catch (Exception e) {
         log.error("Failed directly starting engine {}, retrying", identifier, e);
         return retryEngineStart(identifier, sse);
      }
   }

   private CompletionStage<Void> retryEngineStart(CacheIdentifier identifier, StartStopEngine sse) {
      CompletableFuture<Void> cf = new CompletableFuture<>();
      ReschedulingTask<Boolean> reschedulingTask = new ReschedulingTask<>(scheduledExecutorService,
            () -> {
               try {
                  sse.start();
                  return CompletableFutures.completedTrue();
               } catch (IOException e) {
                  return CompletableFuture.failedFuture(e);
               }
            },
            ignore -> {
               log.info("Engine {} now started", identifier);
               cf.complete(null);
               return true;
            }, 10, TimeUnit.SECONDS,
            t -> {
               log.error("Retry task encountered error for engine {}, rescheduling attempt again later", identifier, t);
               return true;
            });
      sse.markRetrying(reschedulingTask.schedule());
      return cf;
   }

   @Override
   public void removeRule(String name) {
      if (knownRules.remove(name) == null) return;
      remove(identifier -> identifier.rule().equals(name));
   }

   private void remove(Predicate<CacheIdentifier> predicate) {
      for (var it = engines.entrySet().iterator(); it.hasNext();) {
         var entry = it.next();
         CacheIdentifier identifier = entry.getKey();
         if (predicate.test(identifier)) {
            StartStopEngine engine = entry.getValue();
            shutdownEngine(engine, engine.engine.getName());
            it.remove();
         }
      }
   }

   // Accessible during tests.
   enum Status {
      SHUTDOWN,
      RUNNING,
      STOPPING,
      STOPPED,
      RETRYING;
   }

   // Accessible during tests.
   static class StartStopEngine {
      private final EngineWrapper engine;
      private Status status = Status.STOPPED;
      private ReschedulingTask<Boolean> task;

      private StartStopEngine(EngineWrapper engine) {
         this.engine = engine;
      }

      public synchronized void start() throws IOException {
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
