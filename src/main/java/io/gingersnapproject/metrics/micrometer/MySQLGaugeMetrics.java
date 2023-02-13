package io.gingersnapproject.metrics.micrometer;

import io.debezium.connector.mysql.MySqlStreamingChangeEventSourceMetricsMXBean;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;

import java.util.function.Function;
import java.util.function.Supplier;

import static io.gingersnapproject.metrics.micrometer.TagUtil.COMPONENT_KEY;
import static io.gingersnapproject.metrics.micrometer.TagUtil.CONNECTOR_TYPE_KEY;
import static io.gingersnapproject.metrics.micrometer.TagUtil.DEBEZIUM_CONNECTOR;
import static io.gingersnapproject.metrics.micrometer.TagUtil.RULE_KEY;

/**
 * MySQL Database specific metrics.
 */
public enum MySQLGaugeMetrics {

   BIN_LOG_POSITION("log.position", "The most recent position (in bytes) within the binlog that the connector has read", MySqlStreamingChangeEventSourceMetricsMXBean::getBinlogPosition),
   SKIPPED_EVENTS("events.skipped", "The number of events that have been skipped by the MySQL connector", MySqlStreamingChangeEventSourceMetricsMXBean::getNumberOfSkippedEvents),

   DISCONNECTS("disconnects", "The number of disconnects by the MySQL connector", MySqlStreamingChangeEventSourceMetricsMXBean::getNumberOfDisconnects),

   ROLLED_BACK_TXS("transactions.rolledback", "The number of processed transactions that were rolled back and not streamed", MySqlStreamingChangeEventSourceMetricsMXBean::getNumberOfRolledBackTransactions),
   NOT_WELL_FORMED_TXS("transactions.malformed", "The number of transactions that have not conformed to the expected protocol of BEGIN + COMMIT/ROLLBACK", MySqlStreamingChangeEventSourceMetricsMXBean::getNumberOfNotWellFormedTransactions),
   LARGE_TXS("transactions.large", "The number of transactions that have not fit into the look-ahead buffer", MySqlStreamingChangeEventSourceMetricsMXBean::getNumberOfLargeTransactions);

   private final String metricName;
   private final String description;
   private final Function<MySqlStreamingChangeEventSourceMetricsMXBean, Number> mapFunction;

   MySQLGaugeMetrics(String metricName, String description, Function<MySqlStreamingChangeEventSourceMetricsMXBean, Number> mapFunction) {
      this.metricName = "gingersnap.database." + metricName;
      this.description = description;
      this.mapFunction = mapFunction;
   }

   public Meter.Id registerMetric(String rule, Supplier<? extends MySqlStreamingChangeEventSourceMetricsMXBean> supplier, MeterRegistry registry) {
      return Gauge.builder(metricName, MetricUtil.getValue(supplier, mapFunction))
            .tag(COMPONENT_KEY, DEBEZIUM_CONNECTOR)
            .tag(RULE_KEY, rule)
            .tag(CONNECTOR_TYPE_KEY, "mysql")
            .description(description)
            .register(registry)
            .getId();
   }

   public String metricName() {
      return metricName;
   }

}
