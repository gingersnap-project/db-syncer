package io.gingersnapproject.cdc.consumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

import io.gingersnapproject.cdc.EngineWrapper;
import io.gingersnapproject.cdc.chain.Event;
import io.gingersnapproject.cdc.chain.EventContext;
import io.gingersnapproject.cdc.chain.EventProcessingChain;
import io.gingersnapproject.cdc.util.CompletionStages;
import io.gingersnapproject.cdc.util.Serialization;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.infinispan.commons.dataconversion.internal.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<SourceRecord, SourceRecord>> {
   private static final Logger log = LoggerFactory.getLogger(BatchConsumer.class);
   private final EngineWrapper engine;
   private final EventProcessingChain chain;
   private final Executor executor;

   public BatchConsumer(EngineWrapper engine, EventProcessingChain chain, Executor executor) {
      this.chain = chain;
      this.engine = engine;
      this.executor = executor;
   }

   @Override
   public void handleBatch(List<ChangeEvent<SourceRecord, SourceRecord>> records, DebeziumEngine.RecordCommitter<ChangeEvent<SourceRecord, SourceRecord>> committer) {
      log.info("Processing {} entries", records.size());

      try {
         Map<Object, ChangeEvent<SourceRecord, SourceRecord>> acc = new HashMap<>();
         for (ChangeEvent<SourceRecord, SourceRecord> curr : records) {
            acc.compute(curr.value().key(), (key, prev) -> {
               if (prev != null) {
                  uncheckedCommit(prev, committer);
               }
               return curr;
            });
         }

         CompletionStages.join(
               CompletionStages.allOf(
                     acc.values().parallelStream()
                           .map(ev ->
                              CompletableFuture.supplyAsync(() -> {
                                 chain.process(create(ev), new EventContext());
                                 uncheckedCommit(ev, committer);
                                 return null;
                              }, executor))
                           .toArray(CompletionStage[]::new)
               )
         );
         committer.markBatchFinished();
      } catch (Throwable t) {
         log.info("Exception encountered writing updates for engine {}", engine.getName(), t);
         engine.notifyError();
      }
   }

   private Event create(ChangeEvent<SourceRecord, SourceRecord> ev) {
      Json key = create(ev.value().key());
      Json value = create(ev.value().value());
      return new Event(key, value);
   }

   private Json create(Object object) {
      if (object instanceof Struct)
         return Serialization.convert((Struct) object);

      throw new IllegalStateException("Object is not a struct");
   }

   private void uncheckedCommit(ChangeEvent<SourceRecord, SourceRecord> ev, DebeziumEngine.RecordCommitter<ChangeEvent<SourceRecord, SourceRecord>> committer) {
      try {
         committer.markProcessed(ev);
      } catch (InterruptedException e) {
         throw new RuntimeException(e);
      }
   }
}
