package io.gingersnapproject.metrics;

import io.gingersnapproject.metrics.micrometer.MicrometerMetrics;
import io.gingersnapproject.metrics.micrometer.MySQLMetrics;
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
      Stream<Matcher<String>> stream = Stream.empty();
      switch (Profiles.connectorType()) {
         case "mysql":
            stream = Stream.concat(stream, Arrays.stream(MySQLMetrics.values()).map(MetricsResourceTest::convertToContainsString));
            break;
         case "oracle":
            //TODO!
            break;
      }
      var matcherList = Stream.of(
                  stream,
                  Arrays.stream(StreamingMetrics.values()).map(MetricsResourceTest::convertToContainsString),
                  Stream.of(containsString(metricName(MicrometerMetrics.RECONNECT_METRIC_NAME, CACHE_SERVICE, null, Meter.Type.COUNTER, null, false))),
                  Arrays.stream(TimerMetrics.values()).map(MetricsResourceTest::convertToContainsString))
            .flatMap(s -> s)
            .toList();
      given()
            .when().get("/q/metrics")
            .then()
            .body(matcherList.get(0), matcherList.subList(1, matcherList.size()).toArray(Matcher[]::new));
   }

   private static Matcher<String> convertToContainsString(MySQLMetrics metric) {
      return containsString(metricName(metric.metricName(), DEBEZIUM_CONNECTOR, Profiles.connectorType(), Meter.Type.GAUGE, null, true));
   }

   private static Matcher<String> convertToContainsString(StreamingMetrics metric) {
      return containsString(metricName(metric.metricName(), DEBEZIUM_CONNECTOR, Profiles.connectorType(), Meter.Type.GAUGE, null, true));
   }

   private static Matcher<String> convertToContainsString(TimerMetrics metric) {
      return containsString(metricName(metric.metricName(), CACHE_SERVICE, null, Meter.Type.TIMER, "_count", false));
   }

   private static String metricName(String metricName, String component, String connector, Meter.Type type, String suffix, boolean hasRule) {
      String name = NAMING_CONVENTION.name(metricName, type);
      if (suffix != null) {
         name += suffix;
      }

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
