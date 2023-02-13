package io.gingersnapproject.metrics.micrometer;

import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Utilities methods for metrics related code.
 */
public final class MetricUtil {

   private MetricUtil() {
   }

   public static <T> Supplier<Number> getValue(Supplier<? extends T> objSupplier, Function<T, Number> getFunction) {
      return () -> Optional.of(objSupplier.get()).map(getFunction).orElse(-1);
   }

}
