package io.gingersnapproject.metrics.micrometer;

import io.debezium.pipeline.metrics.StreamingChangeEventSourceMetricsMXBean;
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
 * Debezium connector streaming time related metrics (common to all connectors).
 */
public enum StreamingTimeGaugeMetric {
   MILLISECONDS_SINCE_LAST_EVENT("events.last", "The number of milliseconds since the connector has read and processed the most recent event", StreamingChangeEventSourceMetricsMXBean::getMilliSecondsSinceLastEvent),
   MILLISECONDS_BEHIND_SOURCE("source.behind.time", "The number of milliseconds between the last change event’s timestamp and the connector processing it", StreamingChangeEventSourceMetricsMXBean::getMilliSecondsBehindSource),
   ;

   private final String metricName;
   private final String description;
   private final Function<StreamingChangeEventSourceMetricsMXBean, Number> mappingFunction;
   private final TimeUnit timeUnit;

   StreamingTimeGaugeMetric(String metricName, String description, Function<StreamingChangeEventSourceMetricsMXBean, Number> mappingFunction) {
      this(metricName, description, mappingFunction, TimeUnit.MILLISECONDS);
   }

   StreamingTimeGaugeMetric(String metricName, String description, Function<StreamingChangeEventSourceMetricsMXBean, Number> mappingFunction, TimeUnit timeUnit) {
      this.metricName = "gingersnap.database." + metricName;
      this.description = description;
      this.mappingFunction = mappingFunction;
      this.timeUnit = timeUnit;
   }

   public Meter.Id registerMetric(String rule, String connectorType, Supplier<? extends StreamingChangeEventSourceMetricsMXBean> supplier, MeterRegistry registry) {
      return TimeGauge.builder(metricName, MetricUtil.getValue(supplier, mappingFunction), timeUnit)
            .tag(COMPONENT_KEY, DEBEZIUM_CONNECTOR)
            .tag(RULE_KEY, rule)
            .tag(CONNECTOR_TYPE_KEY, connectorType)
            .description(description)
            .register(registry)
            .getId();
   }

   public String metricName() {
      return metricName;
   }

}
