package io.gingersnapproject.metrics.micrometer;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.time.Duration;

/**
 * Cache service timer metrics
 */
public enum TimerMetrics {

   CACHE_PUT_OK("success_cache_puts", "The latency of successful cache service put operations"),
   CACHE_PUT_FAILED("failed_cache_puts", "The latency of failed cache service put operations"),
   CACHE_REMOVE_OK("success_cache_removes", "The latency of successful cache service remove operations"),
   CACHE_REMOVE_FAILED("failed_cache_removes", "The latency of failed cache service remove operations");

   final String metricName;
   final String description;

   TimerMetrics(String metricName, String description) {
      this.metricName = metricName;
      this.description = description;
   }

   public String metricName() {
      return metricName;
   }

   Timer register(MeterRegistry registry) {
      return Timer.builder(metricName)
            .description(description)
            .tags("gingersnap", "cache_service")
            .maximumExpectedValue(Duration.ofSeconds(1))
            .serviceLevelObjectives(
                  Duration.ofMillis(1),
                  Duration.ofMillis(5),
                  Duration.ofMillis(10),
                  Duration.ofMillis(50),
                  Duration.ofMillis(100),
                  Duration.ofMillis(500))
            .publishPercentileHistogram()
            .register(registry);
   }
}
