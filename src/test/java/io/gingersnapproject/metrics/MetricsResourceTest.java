package io.gingersnapproject.metrics;

import io.gingersnapproject.metrics.micrometer.MicrometerMetrics;
import io.gingersnapproject.metrics.micrometer.MySQLMetrics;
import io.gingersnapproject.metrics.micrometer.StreamingMetrics;
import io.gingersnapproject.metrics.micrometer.TagUtil;
import io.gingersnapproject.metrics.micrometer.TimerMetrics;
import io.gingersnapproject.testcontainers.database.MySQL;
import io.gingersnapproject.testcontainers.annotation.WithDatabase;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.config.NamingConvention;
import io.micrometer.prometheus.PrometheusNamingConvention;
import io.quarkus.test.junit.QuarkusTest;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.stream.Stream;

import static io.gingersnapproject.metrics.micrometer.TagUtil.CACHE_SERVICE;
import static io.gingersnapproject.metrics.micrometer.TagUtil.DEBEZIUM_CONNECTOR;
import static io.gingersnapproject.metrics.micrometer.TagUtil.RULE_KEY;
import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.containsString;

@QuarkusTest
@WithDatabase(value = MySQL.class, rules = MetricsResourceTest.RULE)
public class MetricsResourceTest {
   static final String RULE = "metricstest";
   private static final NamingConvention NAMING_CONVENTION = new PrometheusNamingConvention();

   @Test
   public void testMetricsEndpoint() {
      var matcherList = Stream.of(
                  Arrays.stream(MySQLMetrics.values()).map(MetricsResourceTest::convertToContainsString),
                  Arrays.stream(StreamingMetrics.values()).map(MetricsResourceTest::convertToContainsString),
                  Stream.of(containsString(metricName(MicrometerMetrics.RECONNECT_METRIC_NAME, CACHE_SERVICE, Meter.Type.COUNTER, null, false))),
                  Arrays.stream(TimerMetrics.values()).map(MetricsResourceTest::convertToContainsString))
            .flatMap(s -> s)
            .toList();
      given()
            .when().get("/q/metrics")
            .then()
            .body(matcherList.get(0), matcherList.subList(1, matcherList.size()).toArray(Matcher[]::new));
   }

   private static Matcher<String> convertToContainsString(MySQLMetrics metric) {
      return containsString(metricName(metric.metricName(), DEBEZIUM_CONNECTOR, Meter.Type.GAUGE, null, true));
   }

   private static Matcher<String> convertToContainsString(StreamingMetrics metric) {
      return containsString(metricName(metric.metricName(), DEBEZIUM_CONNECTOR, Meter.Type.GAUGE, null, true));
   }

   private static Matcher<String> convertToContainsString(TimerMetrics metric) {
      return containsString(metricName(metric.metricName(), CACHE_SERVICE, Meter.Type.TIMER, "_count", false));
   }

   private static String metricName(String metricName, String component, Meter.Type type, String suffix, boolean hasRule) {
      String name = NAMING_CONVENTION.name(metricName, type);
      if (suffix != null) {
         name += suffix;
      }
      // tags
      name += "{%s=\"%s\",".formatted(
            NAMING_CONVENTION.tagKey(TagUtil.COMPONENT_KEY),
            NAMING_CONVENTION.tagValue(component)
      );
      if (hasRule) {
         name += "%s=\"%s\",".formatted(
               NAMING_CONVENTION.tagKey(RULE_KEY),
               NAMING_CONVENTION.tagValue(RULE)
         );
      }
      name += "}";
      return name;
   }
}
