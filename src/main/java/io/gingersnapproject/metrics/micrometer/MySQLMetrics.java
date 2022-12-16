package io.gingersnapproject.metrics.micrometer;

import io.debezium.connector.mysql.MySqlStreamingChangeEventSourceMetricsMXBean;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

public enum MySQLMetrics {

   BIN_LOG_POSITION("mysql_bin_log_position", "Current MySQL binlog offset position", MySqlStreamingChangeEventSourceMetricsMXBean::getBinlogPosition),
   SKIPPED_EVENTS("mysql_number_of_skipped_events", "Tracks the number of events skipped by underlying mysql-binlog-client", MySqlStreamingChangeEventSourceMetricsMXBean::getNumberOfSkippedEvents),
   DISCONNECTS("mysql_number_of_disconnects", "Tracks the number of times the underlying mysql-binlog-client has been disconnected from MySQL", MySqlStreamingChangeEventSourceMetricsMXBean::getNumberOfDisconnects),
   ROLLED_BACK_TXS("mysql_number_of_rolled_back_transactions", "Tracks the number of rolled back transactions", MySqlStreamingChangeEventSourceMetricsMXBean::getNumberOfRolledBackTransactions),
   NOT_WELL_FORMED_TXS("mysql_number_of_not_well_formed_transactions", "Tracks the number of transactions which are not well-formed", MySqlStreamingChangeEventSourceMetricsMXBean::getNumberOfNotWellFormedTransactions),
   LARGE_TXS("mysql_number_of_large_transactions", "Tracks the number of transaction which contains events that contained more entries than could be contained within the connectors", MySqlStreamingChangeEventSourceMetricsMXBean::getNumberOfLargeTransactions);

   private final String metricNameFormat;
   private final String description;
   private final Function<MySqlStreamingChangeEventSourceMetricsMXBean, Number> mapFunction;

   MySQLMetrics(String metricName, String description, Function<MySqlStreamingChangeEventSourceMetricsMXBean, Number> mapFunction) {
      this.metricNameFormat = "gingersnap_%s_" + metricName;
      this.description = description;
      this.mapFunction = mapFunction;
   }

   public void registerMetric(String rule, Supplier<? extends MySqlStreamingChangeEventSourceMetricsMXBean> supplier, MeterRegistry registry) {
      Gauge.builder(metricName(rule), () -> Optional.ofNullable(supplier.get())
                  .map(mapFunction)
                  .orElse(0))
            .tag("gingersnap", "debezium_connector")
            .description(description())
            .register(registry);
   }

   public String metricName(String rule) {
      return String.format(metricNameFormat, rule);
   }

   public String description() {
      return description;
   }

}
