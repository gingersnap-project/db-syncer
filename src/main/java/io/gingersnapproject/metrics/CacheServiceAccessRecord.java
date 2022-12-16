package io.gingersnapproject.metrics;

import java.util.function.BiConsumer;

/**
 * Records latency access on success or fail.
 *
 * @param <T> The return value.
 */
public interface CacheServiceAccessRecord<T> extends BiConsumer<T, Throwable> {

   /**
    * Records latency for a failed attempt.
    *
    * @param t The {@link Throwable} thrown.
    */
   default void recordThrowable(Throwable t) {
      accept(null, t);
   }

}
