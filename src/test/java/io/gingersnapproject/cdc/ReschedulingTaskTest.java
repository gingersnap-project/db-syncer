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

import io.gingersnapproject.util.ByRef;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ReschedulingTaskTest {

   private final ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor();

   @Test
   public void testRescheduledWhilePredicateFalse() throws Exception {
      ByRef<Integer> value = new ByRef<>(0);
      var latch = new CountDownLatch(10);
      Supplier<CompletionStage<Integer>> supplier = () -> {
         value.setRef(value.ref() + 1);
         latch.countDown();
         return CompletableFuture.completedFuture(value.ref());
      };
      Predicate<Integer> predicate = i -> i > 10;
      var rt = new ReschedulingTask<>(ses, supplier, predicate, 100, TimeUnit.MILLISECONDS, null);

      rt.schedule();

      assert latch.await(2, TimeUnit.SECONDS) : "Task not run";
   }

   @Test
   public void testRescheduledWhileThrowing() throws Exception {
      ByRef<Integer> value = new ByRef<>(0);
      ByRef<Throwable> exception = new ByRef<>(null);
      var latch = new CountDownLatch(10);
      Supplier<CompletionStage<Integer>> supplier = () -> {
         value.setRef(value.ref() + 1);
         latch.countDown();
         if (value.ref() < 10) return CompletableFuture.failedStage(new RuntimeException("Retry too low"));
         return CompletableFuture.completedFuture(value.ref());
      };
      Predicate<Integer> predicate = i -> i > 10;
      var rt = new ReschedulingTask<>(ses, supplier, predicate, 100, TimeUnit.MILLISECONDS, e -> {
         exception.setRef(e);
         return true;
      });

      rt.schedule();

      assert latch.await(2, TimeUnit.SECONDS) : "Task not run";
      assertNotNull(exception.ref(), "Exception not recorded!");
   }
}
