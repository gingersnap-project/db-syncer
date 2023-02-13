package io.gingersnapproject.metrics.micrometer;

import io.gingersnapproject.cdc.connector.DatabaseProvider;
import io.gingersnapproject.cdc.event.Events;
import io.gingersnapproject.metrics.CacheServiceAccessRecord;
import io.gingersnapproject.metrics.DBSyncerMetrics;
import io.gingersnapproject.metrics.GenericStreamingBeanLookup;
import io.gingersnapproject.metrics.MySQLStreamingBeanLookup;
import io.gingersnapproject.metrics.OracleStreamingBeanLookup;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter;
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
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.gingersnapproject.metrics.micrometer.TagUtil.CACHE_SERVICE;
import static io.gingersnapproject.metrics.micrometer.TagUtil.COMPONENT_KEY;

@ApplicationScoped
public class MicrometerMetrics implements DBSyncerMetrics {

   private static final Logger log = LoggerFactory.getLogger(MicrometerMetrics.class);
   public static final String RECONNECT_METRIC_NAME = "gingersnap.reconnects";

   private final MeterRegistry registry;
   private final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
   private final Counter reconnectEvents;
   private final EnumMap<TimerMetrics, Timer> timerMetrics = new EnumMap<>(TimerMetrics.class);
   private final ConcurrentHashMap<String, RuleMetrics> rulesMetric = new ConcurrentHashMap<>();

   public MicrometerMetrics(MeterRegistry registry) {
      this.registry = registry;
      // TODO convert to enum if more the one global counter
      reconnectEvents = Counter.builder(RECONNECT_METRIC_NAME)
            .description("The number of reconnections event to the Cache Service")
            .tag(COMPONENT_KEY, CACHE_SERVICE)
            .register(registry);
      Arrays.stream(TimerMetrics.values()).forEach(metric -> timerMetrics.put(metric, metric.register(registry)));
   }

   void registerMetrics(@Observes Events.ConnectorStartedEvent ev) {
      String rule = ev.identifier().toString();
      DatabaseProvider db = ev.databaseProvider();
      log.info("Registering metrics for {} and connector {}", rule, db);
      if (mBeanServer == null) {
         log.warn("JMX not supported. Metrics for connector {} ({}) are not going to be registered", rule, db);
         return;
      }
      switch (db) {
         case MYSQL -> registerMysqlConnectorMetrics(rule);
         case SQLSERVER -> registerGenericConnector(io.debezium.connector.sqlserver.Module.name(), rule);
         case POSTGRESQL -> registerGenericConnector("postgres", rule);
         case ORACLE -> registerOracleConnectorMetrics(rule);
         default -> log.warn("Unknown database provider: {}", db);
      }
   }

   void unregisterMetrics(@Observes Events.ConnectorStoppedEvent ev) {
      log.info("Unregistering metrics for {}", ev.identifier());
      rulesMetric.computeIfPresent(ev.identifier().toString(), (ruleName, ruleMetrics) -> {
         ruleMetrics.ids().forEach(registry::remove);
         return null;
      });
   }

   void onCacheServiceReconnect(@Observes Events.BackendStartedEvent event) {
      if (event.reconnect()) {
         reconnectEvents.increment();
      }
   }

   private void registerMysqlConnectorMetrics(String name) {
      //mysql connect has a couple extra metrics that we can expose!
      rulesMetric.computeIfAbsent(name, ruleName -> {
         MySQLStreamingBeanLookup lookup;
         try {
            lookup = new MySQLStreamingBeanLookup(ruleName);
         } catch (MalformedObjectNameException e) {
            log.error("Failed to register MySQL metrics for connector {}", ruleName, e);
            return null;
         }
         Stream<Meter.Id> ids1 = Arrays.stream(MySQLGaugeMetrics.values())
               .map(metric -> metric.registerMetric(ruleName, lookup, registry));
         Stream<Meter.Id> ids2 = Arrays.stream(StreamingGaugeMetric.values())
               .map(metric -> metric.registerMetric(ruleName, "mysql", lookup, registry));
         Stream<Meter.Id> ids3 = Arrays.stream(StreamingTimeGaugeMetric.values())
               .map(metric -> metric.registerMetric(ruleName, "mysql", lookup, registry));
         return new RuleMetrics(Stream.of(ids1, ids2, ids3).flatMap(Function.identity()).toList());
      });
   }

   private void registerOracleConnectorMetrics(String name) {
      //oracle connect has a couple extra metrics that we can expose!
      rulesMetric.computeIfAbsent(name, ruleName -> {
         OracleStreamingBeanLookup lookup;
         try {
            lookup = new OracleStreamingBeanLookup(ruleName);
         } catch (MalformedObjectNameException e) {
            log.error("Failed to register Oracle Database metrics for connector {}", ruleName, e);
            return null;
         }
         Stream<Meter.Id> ids1 = Arrays.stream(OracleGaugeMetric.values())
               .map(metric -> metric.registerMetric(ruleName, lookup, registry));
         Stream<Meter.Id> ids2 = Arrays.stream(OracleTimeGaugeMetric.values())
               .map(metric -> metric.registerMetric(ruleName, lookup, registry));
         Stream<Meter.Id> ids3 = Arrays.stream(StreamingGaugeMetric.values())
               .map(metric -> metric.registerMetric(ruleName, "oracle", lookup, registry));
         Stream<Meter.Id> ids4 = Arrays.stream(StreamingTimeGaugeMetric.values())
               .map(metric -> metric.registerMetric(ruleName, "oracle", lookup, registry));

         return new RuleMetrics(Stream.of(ids1, ids2, ids3, ids4).flatMap(Function.identity()).toList());
      });
   }

   private void registerGenericConnector(String type, String name) {
      rulesMetric.computeIfAbsent(name, ruleName -> {
         GenericStreamingBeanLookup lookup;
         try {
            lookup = new GenericStreamingBeanLookup(type, ruleName);
         } catch (MalformedObjectNameException e) {
            log.error("Failed to register {} metrics for connector {}", type, ruleName, e);
            return null;
         }
         var ids1 = Arrays.stream(StreamingGaugeMetric.values())
               .map(metric -> metric.registerMetric(ruleName, type, lookup, registry));
         var ids2 = Arrays.stream(StreamingTimeGaugeMetric.values())
               .map(metric -> metric.registerMetric(ruleName, type, lookup, registry));
         return new RuleMetrics(Stream.concat(ids1, ids2).toList());
      });

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

   private record RuleMetrics(List<Meter.Id> ids) {
   }
}
