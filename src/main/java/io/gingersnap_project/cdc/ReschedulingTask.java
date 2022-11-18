package io.gingersnap_project.cdc;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.jboss.logging.Logger;

public class ReschedulingTask<E> implements AutoCloseable, Runnable {
   private static final Logger log = Logger.getLogger(ReschedulingTask.class);
   private Future<?> future;

   private final ScheduledExecutorService scheduler;
   private final Supplier<CompletionStage<E>> supplier;
   private final Predicate<? super E> predicateConsumer;
   private final long delay;
   private final TimeUnit unit;
   private final Predicate<? super Throwable> mayRepeatOnThrowable;

   public ReschedulingTask(ScheduledExecutorService scheduler, Supplier<CompletionStage<E>> supplier,
         Predicate<? super E> predicateConsumer, long delay, TimeUnit unit, Predicate<? super Throwable> mayRepeatOnThrowable) {
      this.scheduler = scheduler;
      this.supplier = supplier;
      this.predicateConsumer = predicateConsumer;
      this.delay = delay;
      this.unit = unit;
      this.mayRepeatOnThrowable = mayRepeatOnThrowable;
   }

   // Schedules itself with the provided scheduler returning itself
   public synchronized ReschedulingTask<E> schedule() {
      synchronized (this) {
         future = scheduler.schedule(this, delay, unit);
      }
      return this;
   }

   @Override
   public void run() {
      CompletionStage<E> stage;
      try {
         stage = supplier.get();
      } catch (Throwable t) {
         throw t;
      }
      stage.whenComplete((v, t) -> {
         if (t != null) {
            if (mayRepeatOnThrowable == null || !mayRepeatOnThrowable.test(t)) {
               log.errorf("There was an error in submitted periodic task with %s, not rescheduling.", supplier, t);
               return;
            }
            log.tracef(t, "There was an error in submitted periodic non blocking task with supplier %s, configured to resubmit", supplier);
         }
         boolean isRunning;
         synchronized (this) {
            isRunning = future != null;
         }
         if (isRunning) {
            if (t == null && predicateConsumer.test(v)) {
               log.tracef("Predicate consumer notified of completion of supplier %s, not resubmitting", supplier);
               return;
            }
            Future<?> newFuture = scheduler.schedule(this, delay, unit);
            boolean shouldCancel = false;
            synchronized (this) {
               if (future == null) {
                  shouldCancel = true;
               } else {
                  future = newFuture;
               }
            }
            if (shouldCancel) {
               if (log.isTraceEnabled()) {
                  log.tracef("Periodic non blocking task with supplier %s was cancelled while rescheduling.", supplier);
               }
               newFuture.cancel(true);
            }
         } else if (log.isTraceEnabled()) {
            log.tracef("Periodic non blocking task with supplier %s was cancelled prior to execution.", supplier);
         }

      });
   }

   @Override
   public void close() {
      if (log.isTraceEnabled()) {
         log.tracef("Periodic non blocking task with supplier %s was cancelled.", supplier);
      }
      Future<?> cancelFuture;
      synchronized (this) {
         cancelFuture = future;
         future = null;
      }
      if (cancelFuture != null) {
         cancelFuture.cancel(false);
      }
   }
}
