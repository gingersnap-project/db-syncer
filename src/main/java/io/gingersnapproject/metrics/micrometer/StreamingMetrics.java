package io.gingersnapproject.metrics.micrometer;

import io.debezium.pipeline.metrics.StreamingChangeEventSourceMetricsMXBean;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;

import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.gingersnapproject.metrics.micrometer.TagUtil.COMPONENT_KEY;
import static io.gingersnapproject.metrics.micrometer.TagUtil.DEBEZIUM_CONNECTOR;
import static io.gingersnapproject.metrics.micrometer.TagUtil.RULE_KEY;

public enum StreamingMetrics {
   MILLISECONDS_SINCE_LAST_EVENT("database.time.lastevent", "Tracks the last event received", StreamingChangeEventSourceMetricsMXBean::getMilliSecondsSinceLastEvent, -1),
   MILLISECONDS_BEHIND_SOURCE("database.time.lag", "Tracks the lag from the source", StreamingChangeEventSourceMetricsMXBean::getMilliSecondsBehindSource),
   EVENTS("database.events.total", "Tracks the number of events", StreamingChangeEventSourceMetricsMXBean::getTotalNumberOfEventsSeen),
   CREATE_EVENTS("database.events.create", "Tracks the number of create events", StreamingChangeEventSourceMetricsMXBean::getTotalNumberOfCreateEventsSeen),
   UPDATE_EVENTS("database.events.update", "Tracks the number of update events", StreamingChangeEventSourceMetricsMXBean::getTotalNumberOfUpdateEventsSeen),
   DELETE_EVENTS("database.events.delete", "Tracks the number of delete events", StreamingChangeEventSourceMetricsMXBean::getTotalNumberOfDeleteEventsSeen),
   // Because we filter the events manually, I think this will always be 0.
   //EVENTS_FILTERED("number_of_filtered_events", "Tracks the number of filtered events", StreamingChangeEventSourceMetricsMXBean::getNumberOfEventsFiltered),
   ERRONEOUS_EVENTS("database.events.erroneous", "Tracks the number of erroneous events", StreamingChangeEventSourceMetricsMXBean::getNumberOfErroneousEvents),
   // TODO this is static, should it be exposed?
   QUEUE_TOTAL_CAP("database.queue.capacity.total", "The connector's queue max capacity", StreamingChangeEventSourceMetricsMXBean::getQueueTotalCapacity, -1),
   QUEUE_REMAINING_CAP("database.queue.capacity.remaining", "Tracks the remaining capacity in connector's queue", StreamingChangeEventSourceMetricsMXBean::getQueueRemainingCapacity, -1),
   // TODO this is static, should it be exposed?
   QUEUE_MAX_SIZE_BYTES("database.queue.maxsize", "The connector's queue max size in bytes", StreamingChangeEventSourceMetricsMXBean::getMaxQueueSizeInBytes),
   QUEUE_SIZE_BYTES("database.queue.size", "Tracks the queue size in bytes", StreamingChangeEventSourceMetricsMXBean::getCurrentQueueSizeInBytes),
   COMMITTED_TXS("database.transactions.committed", "Tracks the number of committed transactions", StreamingChangeEventSourceMetricsMXBean::getNumberOfCommittedTransactions),
   ;

   private final String metricName;
   private final String description;
   private final Function<StreamingChangeEventSourceMetricsMXBean, Number> mappingFunction;
   private final Number defaultValue;

   StreamingMetrics(String metricName, String description, Function<StreamingChangeEventSourceMetricsMXBean, Number> mappingFunction, Number defaultValue) {
      this.metricName = "gingersnap." + metricName;
      this.description = description;
      this.mappingFunction = mappingFunction;
      this.defaultValue = defaultValue;
   }

   StreamingMetrics(String metricName, String description, Function<StreamingChangeEventSourceMetricsMXBean, Number> mappingFunction) {
      this(metricName, description, mappingFunction, 0);
   }

   public Meter.Id registerMetric(String rule, Supplier<? extends StreamingChangeEventSourceMetricsMXBean> supplier, MeterRegistry registry) {
      return Gauge.builder(metricName, () -> Optional.ofNullable(supplier.get())
                  .map(mappingFunction)
                  .orElse(defaultValue))
            .tag(COMPONENT_KEY, DEBEZIUM_CONNECTOR)
            .tag(RULE_KEY, rule)
            .description(description)
            .register(registry)
            .getId();
   }

   public String metricName() {
      return metricName;
   }
}
