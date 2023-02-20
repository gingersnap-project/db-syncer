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
      Supplier<? extends T> wrap = () -> {
         try {
            return objSupplier.get();
         } catch (Exception e) {
            return null;
         }
      };
      return () -> Optional.ofNullable(wrap.get()).map(getFunction).orElse(-1);
   }

}
