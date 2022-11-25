package io.gingersnapproject.util;

import io.quarkus.arc.Arc;
import io.quarkus.arc.InstanceHandle;

public final class ArcUtil {

   private ArcUtil() { }

   public static <T> T instance(Class<T> clazz) {
      InstanceHandle<T> ih = Arc.container().instance(clazz);
      if (!ih.isAvailable()) {
         throw new RuntimeException("Class " + clazz.getCanonicalName() + " not available in container");
      }
      return ih.get();
   }
}
