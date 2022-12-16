package io.gingersnapproject.metrics.micrometer;

import io.debezium.pipeline.metrics.StreamingChangeEventSourceMetricsMXBean;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

public enum StreamingMetrics {
   MILLISECONDS_SINCE_LAST_EVENT("time_since_last_event_milliseconds", "Tracks the last event received", StreamingChangeEventSourceMetricsMXBean::getMilliSecondsSinceLastEvent, -1),
   EVENTS("number_of_events", "Tracks the number of events", StreamingChangeEventSourceMetricsMXBean::getTotalNumberOfEventsSeen),
   CREATE_EVENTS("number_of_create_events", "Tracks the number of create events", StreamingChangeEventSourceMetricsMXBean::getTotalNumberOfCreateEventsSeen),
   UPDATE_EVENTS("number_of_update_events", "Tracks the number of update events", StreamingChangeEventSourceMetricsMXBean::getTotalNumberOfUpdateEventsSeen),
   DELETE_EVENTS("number_of_delete_events", "Tracks the number of delete events", StreamingChangeEventSourceMetricsMXBean::getTotalNumberOfDeleteEventsSeen),
   // Because we filter the events manually, I think this will always be 0.
   //EVENTS_FILTERED("number_of_filtered_events", "Tracks the number of filtered events", StreamingChangeEventSourceMetricsMXBean::getNumberOfEventsFiltered),
   ERRONEOUS_EVENTS("number_of_erroneous_events", "Tracks the number of erroneous events", StreamingChangeEventSourceMetricsMXBean::getNumberOfErroneousEvents),
   // TODO this is static, should it be exposed?
   QUEUE_TOTAL_CAP("queue_total_capacity", "The connector's queue max capacity", StreamingChangeEventSourceMetricsMXBean::getQueueTotalCapacity, -1),
   QUEUE_REMAINING_CAP("queue_remaining_capacity", "Tracks the remaining capacity in connector's queue", StreamingChangeEventSourceMetricsMXBean::getQueueRemainingCapacity, -1),
   // TODO this is static, should it be exposed?
   QUEUE_MAX_SIZE_BYTES("queue_max_size_bytes", "The connector's queue max size in bytes", StreamingChangeEventSourceMetricsMXBean::getMaxQueueSizeInBytes),
   QUEUE_SIZE_BYTES("queue_size_bytes", "Tracks the queue size in bytes", StreamingChangeEventSourceMetricsMXBean::getCurrentQueueSizeInBytes),
   MILLISECONDS_BEHIND_SOURCE("time_behind_source_milliseconds", "Tracks the lag from the source", StreamingChangeEventSourceMetricsMXBean::getMilliSecondsBehindSource),
   COMMITTED_TXS("number_of_committed_transactions", "Tracks the number of committed transactions", StreamingChangeEventSourceMetricsMXBean::getNumberOfCommittedTransactions),
   ;

   private final String metricNameFormat;
   private final String description;
   private final Function<StreamingChangeEventSourceMetricsMXBean, Number> mappingFunction;
   private final Number defaultValue;

   StreamingMetrics(String metricName, String description, Function<StreamingChangeEventSourceMetricsMXBean, Number> mappingFunction, Number defaultValue) {
      this.metricNameFormat = "gingersnap_%s_" + metricName;
      this.description = description;
      this.mappingFunction = mappingFunction;
      this.defaultValue = defaultValue;
   }

   StreamingMetrics(String metricNameFormat, String description, Function<StreamingChangeEventSourceMetricsMXBean, Number> mappingFunction) {
      this(metricNameFormat, description, mappingFunction, 0);
   }

   public void registerMetric(String rule, Supplier<? extends StreamingChangeEventSourceMetricsMXBean> supplier, MeterRegistry registry) {
      Gauge.builder(metricName(rule), () -> Optional.ofNullable(supplier.get())
                  .map(mappingFunction)
                  .orElse(defaultValue))
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
