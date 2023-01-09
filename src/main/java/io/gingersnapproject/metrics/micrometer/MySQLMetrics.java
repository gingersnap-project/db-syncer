package io.gingersnapproject.metrics.micrometer;

import io.debezium.connector.mysql.MySqlStreamingChangeEventSourceMetricsMXBean;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.gingersnapproject.metrics.micrometer.TagUtil.COMPONENT_KEY;
import static io.gingersnapproject.metrics.micrometer.TagUtil.DEBEZIUM_CONNECTOR;
import static io.gingersnapproject.metrics.micrometer.TagUtil.RULE_KEY;

public enum MySQLMetrics {

   BIN_LOG_POSITION("database.log.position", "Current MySQL binlog offset position", MySqlStreamingChangeEventSourceMetricsMXBean::getBinlogPosition),
   SKIPPED_EVENTS("database.events.skipped", "Tracks the number of events skipped by underlying mysql-binlog-client", MySqlStreamingChangeEventSourceMetricsMXBean::getNumberOfSkippedEvents),
   DISCONNECTS("database.disconnects", "Tracks the number of times the underlying mysql-binlog-client has been disconnected from MySQL", MySqlStreamingChangeEventSourceMetricsMXBean::getNumberOfDisconnects),
   ROLLED_BACK_TXS("database.transactions.rolledback", "Tracks the number of rolled back transactions", MySqlStreamingChangeEventSourceMetricsMXBean::getNumberOfRolledBackTransactions),
   NOT_WELL_FORMED_TXS("database.transactions.malformed", "Tracks the number of transactions which are not well-formed", MySqlStreamingChangeEventSourceMetricsMXBean::getNumberOfNotWellFormedTransactions),
   LARGE_TXS("database.transactions.large", "Tracks the number of transaction which contains events that contained more entries than could be contained within the connectors", MySqlStreamingChangeEventSourceMetricsMXBean::getNumberOfLargeTransactions);

   private final String metricName;
   private final String description;
   private final Function<MySqlStreamingChangeEventSourceMetricsMXBean, Number> mapFunction;

   MySQLMetrics(String metricName, String description, Function<MySqlStreamingChangeEventSourceMetricsMXBean, Number> mapFunction) {
      this.metricName = "gingersnap." + metricName;
      this.description = description;
      this.mapFunction = mapFunction;
   }

   public void registerMetric(String rule, Supplier<? extends MySqlStreamingChangeEventSourceMetricsMXBean> supplier, MeterRegistry registry) {
      Gauge.builder(metricName, () -> Optional.ofNullable(supplier.get())
                  .map(mapFunction)
                  .orElse(0))
            .tag(COMPONENT_KEY, DEBEZIUM_CONNECTOR)
            .tag(RULE_KEY, rule)
            .description(description)
            .register(registry);
   }

   public String metricName() {
      return metricName;
   }

}
