package io.gingersnapproject.cdc;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ReschedulingTaskTest {

   private final ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor();

   @Test
   public void testRescheduledWhilePredicateFalse() throws Exception {
      Ref<Integer> value = new Ref<>(0);
      var latch = new CountDownLatch(10);
      Supplier<CompletionStage<Integer>> supplier = () -> {
         value.ref = value.ref + 1;
         latch.countDown();
         return CompletableFuture.completedFuture(value.ref);
      };
      Predicate<Integer> predicate = i -> i > 10;
      var rt = new ReschedulingTask<>(ses, supplier, predicate, 100, TimeUnit.MILLISECONDS, null);

      rt.schedule();

      assert latch.await(2, TimeUnit.SECONDS) : "Task not run";
   }

   @Test
   public void testRescheduledWhileThrowing() throws Exception {
      Ref<Integer> value = new Ref<>(0);
      Ref<Throwable> exception = new Ref<>(null);
      var latch = new CountDownLatch(10);
      Supplier<CompletionStage<Integer>> supplier = () -> {
         value.ref = value.ref + 1;
         latch.countDown();
         if (value.ref < 10) return CompletableFuture.failedStage(new RuntimeException("Retry too low"));
         return CompletableFuture.completedFuture(value.ref);
      };
      Predicate<Integer> predicate = i -> i > 10;
      var rt = new ReschedulingTask<>(ses, supplier, predicate, 100, TimeUnit.MILLISECONDS, e -> {
         exception.ref = e;
         return true;
      });

      rt.schedule();

      assert latch.await(2, TimeUnit.SECONDS) : "Task not run";
      assertNotNull(exception.ref, "Exception not recorded!");
   }

   private static class Ref<T> {
      protected T ref;

      private Ref(T ref) {
         this.ref = ref;
      }
   }
}
