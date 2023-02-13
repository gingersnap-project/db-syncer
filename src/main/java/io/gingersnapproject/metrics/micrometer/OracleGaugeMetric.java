package io.gingersnapproject.metrics.micrometer;

import io.debezium.connector.oracle.OracleStreamingChangeEventSourceMetricsMXBean;
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
 * Oracle Database specific metric.
 */
public enum OracleGaugeMetric {
   MIN_MINED_LOG_COUNT("mining.log.min", "The minimum number of logs specified for any LogMiner session", OracleStreamingChangeEventSourceMetricsMXBean::getMinimumMinedLogCount),
   MAX_MINED_LOG_COUNT("mining.log.max", "The maximum number of logs specified for any LogMiner session", OracleStreamingChangeEventSourceMetricsMXBean::getMaximumMinedLogCount),
   MINING_USER_BYTES("mining.user.memory", "The current mining session’s user global area (UGA) memory consumption in bytes", OracleStreamingChangeEventSourceMetricsMXBean::getMiningSessionUserGlobalAreaMemoryInBytes, "bytes"),
   MINING_USER_MAX_BYTES("mining.user.memory.max", "The maximum mining session’s user global area (UGA) memory consumption in bytes across all mining sessions", OracleStreamingChangeEventSourceMetricsMXBean::getMiningSessionUserGlobalAreaMaxMemoryInBytes, "bytes"),
   MINING_PROCESS_BYTES("mining.process.memory", "The current mining session’s process global area (PGA) memory consumption in bytes", OracleStreamingChangeEventSourceMetricsMXBean::getMiningSessionProcessGlobalAreaMemoryInBytes, "bytes"),
   MINING_PROCESS_MAX_BYTES("mining.process.memory.max", "The maximum mining session’s process global area (PGA) memory consumption in bytes across all mining sessions", OracleStreamingChangeEventSourceMetricsMXBean::getMiningSessionProcessGlobalAreaMaxMemoryInBytes, "bytes"),

   SWITCH_COUNT("log.switches", "The number of times the database has performed a log switch for the last day", OracleStreamingChangeEventSourceMetricsMXBean::getSwitchCounter),

   DML_COUNT("dml.observed", "The total number of DML operations observed", OracleStreamingChangeEventSourceMetricsMXBean::getTotalCapturedDmlCount),
   DML_MAX_IN_BATCH("dml.batch.max.observed", "The maximum number of DML operations observed while processing a single LogMiner session query", OracleStreamingChangeEventSourceMetricsMXBean::getMaxCapturedDmlInBatch),
   DML_LAST_BATCH_COUNT("dml.batch.last.observed", "The number of DML operations observed in the last LogMiner session query", OracleStreamingChangeEventSourceMetricsMXBean::getLastCapturedDmlCount),
   DML_REGISTERED("dml.registered", "The number of registered DML operations in the transaction buffer", OracleStreamingChangeEventSourceMetricsMXBean::getRegisteredDmlCount),

   QUERY_COUNT("query.performed", "The total number of LogMiner session query (aka batches) performed", OracleStreamingChangeEventSourceMetricsMXBean::getFetchingQueryCount),

   // static configuration but can be changed via JMX
   BATCH_SIZE("batch.size", "The number of entries fetched by the log mining query per database round-trip", OracleStreamingChangeEventSourceMetricsMXBean::getBatchSize),
   BATCH_MAX_THROUGHPUT("batch.throughput.max", "The maximum number of rows/second processed from the log mining view", OracleStreamingChangeEventSourceMetricsMXBean::getMaxBatchProcessingThroughput),
   BATCH_AVG_THROUGHPUT("batch.throughput.avg", "The average number of rows/second processed from the log mining", OracleStreamingChangeEventSourceMetricsMXBean::getAverageBatchProcessingThroughput),
   BATCH_LAST_THROUGHPUT("batch.throughput.last.avg", "The average number of rows/second processed from the log mining view for the last batch", OracleStreamingChangeEventSourceMetricsMXBean::getLastBatchProcessingThroughput),

   NETWORK_ERRORS("errors.network", "The number of connection problems detected", OracleStreamingChangeEventSourceMetricsMXBean::getNetworkConnectionProblemsCounter),
   ERROR_COUNT("errors", "The number of errors detected", OracleStreamingChangeEventSourceMetricsMXBean::getErrorCount),
   WARN_COUNT("warns", "The number of warnings detected", OracleStreamingChangeEventSourceMetricsMXBean::getWarningCount),

   UNPARSABLE_DDL("parse.ddl.failed", "The number of DDL records that have been detected but could not be parsed by the DDL parser", OracleStreamingChangeEventSourceMetricsMXBean::getUnparsableDdlCount),

   ROWS_PROCESSED("rows.processed", "The total number of rows processed from the log mining view across all sessions", OracleStreamingChangeEventSourceMetricsMXBean::getTotalProcessedRows),

   TX_ACTIVE("transaction.active", "The number of current active transactions in the transaction buffer", OracleStreamingChangeEventSourceMetricsMXBean::getNumberOfActiveTransactions),
   TX_ROLLBACK("transaction.rollback", "The number of rolled back transactions in the transaction buffer", OracleStreamingChangeEventSourceMetricsMXBean::getNumberOfRolledBackTransactions),
   TX_OVERSIZE("transaction.oversize", "The number of transactions that were discarded because their size exceeded threshold", OracleStreamingChangeEventSourceMetricsMXBean::getNumberOfOversizedTransactions),
   TX_COMMIT_THROUGHPUT("transaction.commit.throughput", "The average number of committed transactions per second in the transaction buffer", OracleStreamingChangeEventSourceMetricsMXBean::getCommitThroughput),

   SCN_FREEZE("scn.freezes", "The number of times that the system change number was checked for advancement and remains unchanged", OracleStreamingChangeEventSourceMetricsMXBean::getScnFreezeCount),
   ;

   private final String metricName;
   private final String description;
   private final Function<OracleStreamingChangeEventSourceMetricsMXBean, Number> mapFunction;
   private final String baseUnit;

   OracleGaugeMetric(String metricName, String description, Function<OracleStreamingChangeEventSourceMetricsMXBean, Number> mapFunction) {
      this(metricName, description, mapFunction, null);
   }

   OracleGaugeMetric(String metricName, String description, Function<OracleStreamingChangeEventSourceMetricsMXBean, Number> mapFunction, String baseUnit) {
      this.metricName = "gingersnap.database." + metricName;
      this.description = description;
      this.mapFunction = mapFunction;
      this.baseUnit = baseUnit;
   }

   public Meter.Id registerMetric(String rule, Supplier<? extends OracleStreamingChangeEventSourceMetricsMXBean> supplier, MeterRegistry registry) {
      return Gauge.builder(metricName, MetricUtil.getValue(supplier, mapFunction))
            .tag(COMPONENT_KEY, DEBEZIUM_CONNECTOR)
            .tag(RULE_KEY, rule)
            .tag(CONNECTOR_TYPE_KEY, "oracle")
            .baseUnit(baseUnit)
            .description(description)
            .register(registry)
            .getId();
   }

   public String metricName() {
      return metricName;
   }

   public String baseUnit() {
      return baseUnit == null ? null : baseUnit.toString().toLowerCase();
   }
}
