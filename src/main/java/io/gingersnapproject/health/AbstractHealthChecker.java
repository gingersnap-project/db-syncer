package io.gingersnapproject.health;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.HealthCheckResponseBuilder;

public abstract class AbstractHealthChecker<ID> implements HealthCheck, Function<ID, String> {

   private final Map<ID, Boolean> statuses = new ConcurrentHashMap<>();

   protected final void isUp(ID id) {
      statuses.put(id, true);
   }

   protected final void isDown(ID id) {
      statuses.put(id, false);
   }

   protected final void isDone(ID id) {
      statuses.remove(id);
   }

   protected abstract String name();

   @Override
   public HealthCheckResponse call() {
      HealthCheckResponseBuilder builder = HealthCheckResponse.named(name());
      boolean status = !statuses.isEmpty();
      for (Map.Entry<ID, Boolean> entry : statuses.entrySet()) {
         builder.withData(apply(entry.getKey()), entry.getValue());
         status = status && entry.getValue();
      }

      return builder.status(status).build();
   }
}
