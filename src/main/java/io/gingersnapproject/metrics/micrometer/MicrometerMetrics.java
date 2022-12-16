package io.gingersnapproject.metrics.micrometer;

import io.gingersnapproject.cdc.connector.DatabaseProvider;
import io.gingersnapproject.cdc.event.Events;
import io.gingersnapproject.metrics.CacheServiceAccessRecord;
import io.gingersnapproject.metrics.DBSyncerMetrics;
import io.gingersnapproject.metrics.GenericStreamingBeanLookup;
import io.gingersnapproject.metrics.MySQLStreamingBeanLookup;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.concurrent.TimeUnit;

@ApplicationScoped
public class MicrometerMetrics implements DBSyncerMetrics {

   private static final Logger log = LoggerFactory.getLogger(MicrometerMetrics.class);
   public static final String RECONNECT_METRIC_NAME = "number_of_reconnects";

   private final MeterRegistry registry;
   private final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
   private final Counter reconnectEvents;
   private final EnumMap<TimerMetrics, Timer> timerMetrics = new EnumMap<>(TimerMetrics.class);

   public MicrometerMetrics(MeterRegistry registry) {
      this.registry = registry;
      // TODO convert to enum if more the one global counter
      reconnectEvents = Counter.builder(RECONNECT_METRIC_NAME)
            .description("The number of reconnections event to the Cache Service")
            .tag("gingersnap", "cache_service")
            .register(registry);
      Arrays.stream(TimerMetrics.values()).forEach(metric -> timerMetrics.put(metric, metric.register(registry)));
   }

   void registerMetrics(@Observes Events.ConnectorStartedEvent ev) {
      String rule = ev.name();
      DatabaseProvider db = ev.databaseProvider();
      log.info("Registering metrics for {}", rule);
      if (mBeanServer == null) {
         log.warn("JMX not supported. Metrics for connector {} ({}) are not going to be registered", rule, db);
         return;
      }
      switch (db) {
         case MYSQL -> registerMysqlConnectorMetrics(rule);
         case SQLSERVER -> registerGenericConnector(io.debezium.connector.sqlserver.Module.name(), rule);
         case POSTGRESQL -> registerGenericConnector(io.debezium.connector.postgresql.Module.name(), rule);
         default -> log.warn("Unknown database provider: {}", db);
      }
   }

   void unregisterMetrics(@Observes Events.ConnectorStoppedEvent ev) {
      log.info("Unregistering metrics for {}", ev.name());
      //TODO remove metrics
   }

   void onCacheServiceReconnect(@Observes Events.BackendStartedEvent event) {
      if (event.reconnect()) {
         reconnectEvents.increment();
      }
   }

   private void registerMysqlConnectorMetrics(String name) {
      //mysql connect has a couple extra metrics that we can expose!
      MySQLStreamingBeanLookup lookup;
      try {
         lookup = new MySQLStreamingBeanLookup(name);
      } catch (MalformedObjectNameException e) {
         log.error("Failed to register MySQL metrics for connector {}", name, e);
         return;
      }
      for (MySQLMetrics metric : MySQLMetrics.values()) {
         metric.registerMetric(name, lookup, registry);
      }
      for (StreamingMetrics metric : StreamingMetrics.values()) {
         metric.registerMetric(name, lookup, registry);
      }
   }

   private void registerGenericConnector(String type, String name) {
      GenericStreamingBeanLookup lookup;
      try {
         lookup = new GenericStreamingBeanLookup(type, name);
      } catch (MalformedObjectNameException e) {
         log.error("Failed to register {} metrics for connector {}", type, name, e);
         return;
      }
      for (StreamingMetrics metric : StreamingMetrics.values()) {
         metric.registerMetric(name, lookup, registry);
      }
   }

   @Override
   public <T> CacheServiceAccessRecord<T> recordCacheServicePut() {
      return new AccessRecordImpl<>(System.nanoTime(), timerMetrics, TimerMetrics.CACHE_PUT_OK, TimerMetrics.CACHE_REMOVE_OK);
   }

   @Override
   public <T> CacheServiceAccessRecord<T> recordCacheServiceRemove() {
      return new AccessRecordImpl<>(System.nanoTime(), timerMetrics, TimerMetrics.CACHE_REMOVE_OK, TimerMetrics.CACHE_REMOVE_FAILED);
   }

   private record AccessRecordImpl<T>(long startNanos, EnumMap<TimerMetrics, Timer> timeMetrics, TimerMetrics success,
                                      TimerMetrics failed) implements CacheServiceAccessRecord<T> {

      @Override
      public void accept(T t, Throwable throwable) {
         (throwable == null ? timeMetrics.get(success) : timeMetrics.get(failed)).record(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);
      }
   }
}
