package io.gingersnapproject.metrics;

import io.gingersnapproject.metrics.micrometer.MicrometerMetrics;
import io.gingersnapproject.metrics.micrometer.MySQLMetrics;
import io.gingersnapproject.metrics.micrometer.OracleGaugeMetric;
import io.gingersnapproject.metrics.micrometer.OracleTimeGaugeMetric;
import io.gingersnapproject.metrics.micrometer.StreamingMetrics;
import io.gingersnapproject.metrics.micrometer.TimerMetrics;
import io.gingersnapproject.testcontainers.Profiles;
import io.gingersnapproject.testcontainers.annotation.WithDatabase;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.config.NamingConvention;
import io.micrometer.prometheus.PrometheusNamingConvention;
import io.quarkus.test.junit.QuarkusTest;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.gingersnapproject.metrics.micrometer.TagUtil.CACHE_SERVICE;
import static io.gingersnapproject.metrics.micrometer.TagUtil.COMPONENT_KEY;
import static io.gingersnapproject.metrics.micrometer.TagUtil.CONNECTOR_TYPE_KEY;
import static io.gingersnapproject.metrics.micrometer.TagUtil.DEBEZIUM_CONNECTOR;
import static io.gingersnapproject.metrics.micrometer.TagUtil.RULE_KEY;
import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.containsString;

@QuarkusTest
@WithDatabase(rules = MetricsResourceTest.RULE)
public class MetricsResourceTest {
   static final String RULE = "metricstest";
   private static final NamingConvention NAMING_CONVENTION = new PrometheusNamingConvention();

   @Test
   public void testMetricsEndpoint() {
      Stream<String> streamingMetrics = switch (Profiles.connectorType()) {
         case "mysql" -> Stream.concat(
               Arrays.stream(StreamingMetrics.values()).map(MetricsResourceTest::toMetricId),
               Arrays.stream(MySQLMetrics.values()).map(MetricsResourceTest::toMetricId));
         case "oracle" -> Stream.of(
                     Arrays.stream(StreamingMetrics.values()).map(MetricsResourceTest::toMetricId),
                     Arrays.stream(OracleGaugeMetric.values()).map(MetricsResourceTest::toMetricId),
                     Arrays.stream(OracleTimeGaugeMetric.values()).map(MetricsResourceTest::toMetricId))
               .flatMap(Function.identity());
         default -> Arrays.stream(StreamingMetrics.values()).map(MetricsResourceTest::toMetricId);
      };
      var matcherList = Stream.of(
                  streamingMetrics.map(MetricsResourceTest::connectorMetricMatcher),
                  Stream.of(containsString(metricName(toCounterMetricId(MicrometerMetrics.RECONNECT_METRIC_NAME), CACHE_SERVICE, null, false))),
                  Arrays.stream(TimerMetrics.values()).map(MetricsResourceTest::convertToContainsString))
            .flatMap(s -> s)
            .toList();
      given()
            .when().get("/q/metrics")
            .then()
            .body(matcherList.get(0), matcherList.subList(1, matcherList.size()).toArray(Matcher[]::new));
   }

   private static String toMetricId(StreamingMetrics metric) {
      return toGaugeMetricId(metric.metricName(), null);
   }

   private static String toMetricId(MySQLMetrics metric) {
      return toGaugeMetricId(metric.metricName(), null);
   }

   private static String toMetricId(OracleGaugeMetric metric) {
      return toGaugeMetricId(metric.metricName(), metric.baseUnit());
   }

   private static String toMetricId(OracleTimeGaugeMetric metric) {
      // time gauges are reported in seconds by prometheus
      return toGaugeMetricId(metric.metricName(), "seconds");
   }

   private static String toGaugeMetricId(String name, String baseUnit) {
      return NAMING_CONVENTION.name(name, Meter.Type.GAUGE, baseUnit);
   }

   private static String toTimerMetricId(String name) {
      return NAMING_CONVENTION.name(name, Meter.Type.TIMER) + "_count";
   }

   private static String toCounterMetricId(String name) {
      return NAMING_CONVENTION.name(name, Meter.Type.COUNTER);
   }

   private static Matcher<String> connectorMetricMatcher(String metricId) {
      return containsString(metricName(metricId, DEBEZIUM_CONNECTOR, Profiles.connectorType(), true));
   }

   private static Matcher<String> convertToContainsString(TimerMetrics metric) {
      return containsString(metricName(toTimerMetricId(metric.metricName()), CACHE_SERVICE, null, false));
   }

   private static String metricName(String metricId, String component, String connector, boolean hasRule) {
      String name = metricId;

      // tags
      // tags are sorted by key
      var tags = new TreeMap<>();
      // component tag is present in all our metrics
      tags.put(NAMING_CONVENTION.tagKey(COMPONENT_KEY), NAMING_CONVENTION.tagValue(component));

      if (connector != null) {
         tags.put(NAMING_CONVENTION.tagKey(CONNECTOR_TYPE_KEY), NAMING_CONVENTION.tagValue(connector));
      }

      if (hasRule) {
         tags.put(NAMING_CONVENTION.tagKey(RULE_KEY), NAMING_CONVENTION.tagValue(RULE + "-localhost"));
      }
      name += "{" +
            tags.entrySet().stream()
                  .map(e -> "%s=\"%s\"".formatted(e.getKey(), e.getValue()))
                  .collect(Collectors.joining(",")) +
            "}";
      return name;
   }
}
