package io.gingersnapproject.metrics;

import io.gingersnapproject.metrics.micrometer.MicrometerMetrics;
import io.gingersnapproject.metrics.micrometer.MySQLMetrics;
import io.gingersnapproject.metrics.micrometer.StreamingMetrics;
import io.gingersnapproject.metrics.micrometer.TimerMetrics;
import io.gingersnapproject.testcontainers.MySQLResources;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.stream.Stream;

import static io.restassured.RestAssured.given;
import static java.lang.String.format;
import static org.hamcrest.CoreMatchers.containsString;

@QuarkusTest
@QuarkusTestResource(MySQLResources.class)
public class MetricsResourceTest {

   private static final String DEBEZIUM_METRICS = "%s{gingersnap=\"debezium_connector\",}";
   private static final String COUNTER_CACHE_SERVICE_METRICS = "%s_total{gingersnap=\"cache_service\",}";
   private static final String TIMER_CACHE_SERVICE_METRICS = "%s_seconds_count{gingersnap=\"cache_service\",}";

   @Test
   public void testMetricsEndpoint() {
      var matcherList = Stream.of(
                  Arrays.stream(MySQLMetrics.values()).map(MetricsResourceTest::convertToContainsString),
                  Arrays.stream(StreamingMetrics.values()).map(MetricsResourceTest::convertToContainsString),
                  Stream.of(containsString(COUNTER_CACHE_SERVICE_METRICS.formatted(MicrometerMetrics.RECONNECT_METRIC_NAME))),
                  Arrays.stream(TimerMetrics.values()).map(MetricsResourceTest::convertToContainsString))
            .flatMap(s -> s)
            .toList();
      given()
            .when().get("/q/metrics")
            .then()
            .body(matcherList.get(0), matcherList.subList(1, matcherList.size()).toArray(Matcher[]::new));
   }

   private static Matcher<String> convertToContainsString(MySQLMetrics metric) {
      return containsString(format(DEBEZIUM_METRICS, metric.metricName(MySQLResources.RULE).replace('-', '_')));
   }

   private static Matcher<String> convertToContainsString(StreamingMetrics metric) {
      return containsString(format(DEBEZIUM_METRICS, metric.metricName(MySQLResources.RULE).replace('-', '_')));
   }

   private static Matcher<String> convertToContainsString(TimerMetrics metric) {
      return containsString(format(TIMER_CACHE_SERVICE_METRICS, metric.metricName()));
   }
}
