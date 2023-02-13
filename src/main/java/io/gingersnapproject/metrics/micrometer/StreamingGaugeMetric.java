package io.gingersnapproject.metrics.micrometer;

import io.debezium.pipeline.metrics.StreamingChangeEventSourceMetricsMXBean;
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
 * Debezium connector streaming metrics (common to all connectors).
 */
public enum StreamingGaugeMetric {
   EVENTS("events", "The total number of events that this connector has seen since the last start or metrics reset", StreamingChangeEventSourceMetricsMXBean::getTotalNumberOfEventsSeen),
   CREATE_EVENTS("events.created", "The total number of create events that this connector has seen since the last start or metrics reset", StreamingChangeEventSourceMetricsMXBean::getTotalNumberOfCreateEventsSeen),
   UPDATE_EVENTS("events.updated", "The total number of update events that this connector has seen since the last start or metrics reset", StreamingChangeEventSourceMetricsMXBean::getTotalNumberOfUpdateEventsSeen),
   DELETE_EVENTS("events.deleted", "The total number of delete events that this connector has seen since the last start or metrics reset", StreamingChangeEventSourceMetricsMXBean::getTotalNumberOfDeleteEventsSeen),
   ERRONEOUS_EVENTS("events.erroneous", "The total number of erroneous events", StreamingChangeEventSourceMetricsMXBean::getNumberOfErroneousEvents),

   // Because we filter the events manually, I think this will always be 0.
   //EVENTS_FILTERED("number_of_filtered_events", "Tracks the number of filtered events", StreamingChangeEventSourceMetricsMXBean::getNumberOfEventsFiltered),

   // TODO this is static, should it be exposed?
   QUEUE_TOTAL_CAP("queue.total.capacity", "The length the queue used to pass events between the streamer and the main Kafka Connect loop", StreamingChangeEventSourceMetricsMXBean::getQueueTotalCapacity),
   QUEUE_REMAINING_CAP("queue.remaining.capacity", "The free capacity of the queue used to pass events between the streamer and the main Kafka Connect loop", StreamingChangeEventSourceMetricsMXBean::getQueueRemainingCapacity),
   // TODO this is static, should it be exposed?
   QUEUE_MAX_SIZE_BYTES("queue.size.max", "The maximum buffer of the queue in bytes", StreamingChangeEventSourceMetricsMXBean::getMaxQueueSizeInBytes, "bytes"),
   QUEUE_SIZE_BYTES("queue.size.current", "The current volume, in bytes, of records in the queue", StreamingChangeEventSourceMetricsMXBean::getCurrentQueueSizeInBytes, "bytes"),

   TXS_COMMITTED("transactions.committed", "The number of processed transactions that were committed", StreamingChangeEventSourceMetricsMXBean::getNumberOfCommittedTransactions),
   ;

   private final String metricName;
   private final String description;
   private final Function<StreamingChangeEventSourceMetricsMXBean, Number> mappingFunction;
   private final String baseUnit;

   StreamingGaugeMetric(String metricName, String description, Function<StreamingChangeEventSourceMetricsMXBean, Number> mappingFunction, String baseUnit) {
      this.metricName = "gingersnap.database." + metricName;
      this.description = description;
      this.mappingFunction = mappingFunction;
      this.baseUnit = baseUnit;
   }

   StreamingGaugeMetric(String metricName, String description, Function<StreamingChangeEventSourceMetricsMXBean, Number> mappingFunction) {
      this(metricName, description, mappingFunction, null);
   }

   public Meter.Id registerMetric(String rule, String connectorType, Supplier<? extends StreamingChangeEventSourceMetricsMXBean> supplier, MeterRegistry registry) {
      return Gauge.builder(metricName, MetricUtil.getValue(supplier, mappingFunction))
            .tag(COMPONENT_KEY, DEBEZIUM_CONNECTOR)
            .tag(RULE_KEY, rule)
            .tag(CONNECTOR_TYPE_KEY, connectorType)
            .baseUnit(baseUnit)
            .description(description)
            .register(registry)
            .getId();
   }

   public String metricName() {
      return metricName;
   }

   public String baseUnit() {
      return baseUnit;
   }
}
