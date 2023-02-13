package io.gingersnapproject.metrics.micrometer;

import io.debezium.connector.oracle.OracleStreamingChangeEventSourceMetricsMXBean;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.TimeGauge;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.gingersnapproject.metrics.micrometer.TagUtil.COMPONENT_KEY;
import static io.gingersnapproject.metrics.micrometer.TagUtil.CONNECTOR_TYPE_KEY;
import static io.gingersnapproject.metrics.micrometer.TagUtil.DEBEZIUM_CONNECTOR;
import static io.gingersnapproject.metrics.micrometer.TagUtil.RULE_KEY;

/**
 * Oracle Database time related specific metrics.
 */
public enum OracleTimeGaugeMetric {
   MINING_TOTAL_START_TIME("mining.start.total.time", "The total duration in milliseconds spent by the connector starting LogMiner sessions", OracleStreamingChangeEventSourceMetricsMXBean::getTotalMiningSessionStartTimeInMilliseconds),
   MINING_LAST_START_TIME("mining.start.last.time", "The duration in milliseconds to start the last LogMiner session", OracleStreamingChangeEventSourceMetricsMXBean::getLastMiningSessionStartTimeInMilliseconds),
   MINING_MAX_START_TIME("mining.start.max.time", "The longest duration in milliseconds to start a LogMiner session", OracleStreamingChangeEventSourceMetricsMXBean::getMaxMiningSessionStartTimeInMilliseconds),

   // static configuration but can be changed via JMX
   QUERY_SLEEP_TIME("query.sleep.time", "The number of milliseconds the connector sleeps before fetching another batch of results from the log mining view", OracleStreamingChangeEventSourceMetricsMXBean::getMillisecondToSleepBetweenMiningQuery),
   QUERY_MAX_DURATION("query.max.duration", "The maximum duration of any LogMiner session query’s fetch in milliseconds", OracleStreamingChangeEventSourceMetricsMXBean::getMaxDurationOfFetchQueryInMilliseconds),
   QUERY_LAST_DURATION("query.last.duration", "The duration of the last LogMiner session query’s fetch in milliseconds", OracleStreamingChangeEventSourceMetricsMXBean::getLastDurationOfFetchQueryInMilliseconds),

   BATCH_LAST_DURATION("batch.last.duration", "The duration for processing the last LogMiner query batch results in milliseconds", OracleStreamingChangeEventSourceMetricsMXBean::getLastBatchProcessingTimeInMilliseconds),
   BATCH_MIN_TIME("batch.process.min.time", "The total duration in milliseconds spent processing results from LogMiner sessions", OracleStreamingChangeEventSourceMetricsMXBean::getMinBatchProcessingTimeInMilliseconds),
   BATCH_MAX_TIME("batch.process.max.time", "The maximum duration in milliseconds spent processing results from a single LogMiner session", OracleStreamingChangeEventSourceMetricsMXBean::getMaxBatchProcessingTimeInMilliseconds),

   PARSE_TOTAL_TIME("parse.total.time", "The time in milliseconds spent parsing DML event SQL statements", OracleStreamingChangeEventSourceMetricsMXBean::getTotalParseTimeInMilliseconds),

   PROCESSING_TIME("result.process.total.time", "The total duration in milliseconds spent processing results from LogMiner sessions", OracleStreamingChangeEventSourceMetricsMXBean::getTotalProcessingTimeInMilliseconds),

   LAG_TIME("lag.time", "The time difference in milliseconds between when a change occurred in the transaction logs and when its added to the transaction buffer", OracleStreamingChangeEventSourceMetricsMXBean::getLagFromSourceInMilliseconds),
   LAG_MAX_TIME("lag.max.time", "The maximum time difference in milliseconds between when a change occurred in the transaction logs and when its added to the transaction buffer", OracleStreamingChangeEventSourceMetricsMXBean::getMaxLagFromSourceInMilliseconds),
   LAG_MIN_TIME("lag.min.time", "The minimum time difference in milliseconds between when a change occurred in the transaction logs and when its added to the transaction buffer", OracleStreamingChangeEventSourceMetricsMXBean::getMinLagFromSourceInMilliseconds),

   ROWS_NEXT_TOTAL_TIME("rows.next.total.time", "The total duration in milliseconds spent by the JDBC driver fetching the next row to be processed from the log mining view", OracleStreamingChangeEventSourceMetricsMXBean::getTotalResultSetNextTimeInMilliseconds),

   TX_HOURS_IN_BUFFER("transaction.retained.time", "The number of hours that transactions are retained by the connector’s in-memory buffer without being committed or rolled back before being discarded", OracleStreamingChangeEventSourceMetricsMXBean::getHoursToKeepTransactionInBuffer, TimeUnit.HOURS),
   TX_COMMIT_DURATION("transaction.commit.last.duration", "The duration of the last transaction buffer commit operation in milliseconds", OracleStreamingChangeEventSourceMetricsMXBean::getLastCommitDurationInMilliseconds),
   TX_COMMIT_MAX_DURATION("transaction.commit.max.duration", "The duration of the longest transaction buffer commit operation in milliseconds", OracleStreamingChangeEventSourceMetricsMXBean::getMaxCommitDurationInMilliseconds),
   ;

   private final String metricName;
   private final String description;
   private final Function<OracleStreamingChangeEventSourceMetricsMXBean, Number> mapFunction;
   private final TimeUnit timeUnit;

   OracleTimeGaugeMetric(String metricName, String description, Function<OracleStreamingChangeEventSourceMetricsMXBean, Number> mapFunction) {
      // most common metrics time unit!
      this(metricName, description, mapFunction, TimeUnit.MILLISECONDS);
   }

   OracleTimeGaugeMetric(String metricName, String description, Function<OracleStreamingChangeEventSourceMetricsMXBean, Number> mapFunction, TimeUnit timeUnit) {
      this.metricName = "gingersnapn.database." + metricName;
      this.description = description;
      this.mapFunction = mapFunction;
      this.timeUnit = timeUnit;
   }

   public Meter.Id registerMetric(String rule, Supplier<? extends OracleStreamingChangeEventSourceMetricsMXBean> supplier, MeterRegistry registry) {
      return TimeGauge.builder(metricName, MetricUtil.getValue(supplier, mapFunction), timeUnit)
            .tag(COMPONENT_KEY, DEBEZIUM_CONNECTOR)
            .tag(RULE_KEY, rule)
            .tag(CONNECTOR_TYPE_KEY, "oracle")
            .description(description)
            .register(registry)
            .getId();
   }

   public String metricName() {
      return metricName;
   }
}
