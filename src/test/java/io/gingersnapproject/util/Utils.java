package io.gingersnapproject.util;

import static org.junit.jupiter.api.Assertions.fail;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import io.quarkus.arc.ClientProxy;
import org.junit.platform.commons.util.ReflectionUtils;

public class Utils {

   private Utils() { }

   public static <R, O> R extractField(O instance, String field) {
      return extractField(instance.getClass(), field, instance);
   }

   @SuppressWarnings("unchecked")
   public static <R, O> R extractField(Class<? extends O> clazz, String field, O instance) {
      var value = ReflectionUtils.tryToReadFieldValue((Class<O>) clazz, field, unwrap(instance));
      return (R) value.getOrThrow(e ->
            new RuntimeException(String.format("Unable to read field '%s' of %s", field, clazz.getSimpleName()), e));
   }

   public static <R, O> R invokePrivateMethod(O instance, String method, Object ... args) {
      var parameterTypes = new Class[args.length];
      for (int i = 0; i < args.length; i++) {
         parameterTypes[i] = args[i].getClass();
      }
      var optional = ReflectionUtils.findMethod(instance.getClass(), method, parameterTypes);
      return optional
            .map(m -> {
               m.trySetAccessible();
               try {
                  return (R) m.invoke(instance, args);
               } catch (IllegalAccessException | InvocationTargetException e) {
                  return null;
               }
            })
            .orElseThrow(() -> new RuntimeException(String.format("Method '%s' not found", method)));
   }

   public static void eventually(Supplier<String> messageSupplier, Runnable condition, long timeout, TimeUnit timeUnit) {
      eventually(messageSupplier, () -> {
         try {
            condition.run();
            return true;
         } catch (Throwable ignore) {
            return false;
         }
      }, timeout, timeUnit);
   }

   public static void eventually(Supplier<String> messageSupplier, BooleanSupplier condition, long timeout, TimeUnit timeUnit) {
      try {
         long timeoutNanos = timeUnit.toNanos(timeout);
         // We want the sleep time to increase in arithmetic progression
         // 30 loops with the default timeout of 30 seconds means the initial wait is ~ 65 millis
         int loops = 30;
         int progressionSum = loops * (loops + 1) / 2;
         long initialSleepNanos = timeoutNanos / progressionSum;
         long sleepNanos = initialSleepNanos;
         long expectedEndTime = System.nanoTime() + timeoutNanos;
         while (expectedEndTime - System.nanoTime() > 0) {
            if (condition.getAsBoolean())
               return;
            LockSupport.parkNanos(sleepNanos);
            sleepNanos += initialSleepNanos;
         }
         assert condition.getAsBoolean() : fail(messageSupplier.get());
      } catch (Exception e) {
         throw new RuntimeException("Unexpected!", e);
      }
   }

   private static <T> T unwrap(T instance) {
      for (Class<?> itf : instance.getClass().getInterfaces()) {
         if (itf.isAssignableFrom(ClientProxy.class)) {
            var proxy = (ClientProxy) instance;
            return (T) proxy.arc_contextualInstance();
         }
      }
      return instance;
   }
}
