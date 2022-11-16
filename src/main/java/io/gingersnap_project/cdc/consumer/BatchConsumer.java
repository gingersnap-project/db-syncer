package io.gingersnap_project.cdc.consumer;

import java.util.List;

import io.gingersnap_project.cdc.EngineWrapper;
import io.gingersnap_project.cdc.chain.Event;
import io.gingersnap_project.cdc.chain.EventContext;
import io.gingersnap_project.cdc.chain.EventProcessingChain;
import io.gingersnap_project.cdc.util.Serialization;

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

   public BatchConsumer(EngineWrapper engine, EventProcessingChain chain) {
      this.chain = chain;
      this.engine = engine;
   }

   @Override
   public void handleBatch(List<ChangeEvent<SourceRecord, SourceRecord>> records, DebeziumEngine.RecordCommitter<ChangeEvent<SourceRecord, SourceRecord>> committer) {
      log.info("Processing {} entries", records.size());

      try {
         for (ChangeEvent<SourceRecord, SourceRecord> ev : records) {
            // TODO: we may be able to do this in parallel later or asynchronously even
            chain.process(create(ev), new EventContext());
            committer.markProcessed(ev);
         }
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
}
