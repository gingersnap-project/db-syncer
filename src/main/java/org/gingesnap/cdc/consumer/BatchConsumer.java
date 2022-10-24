package org.gingesnap.cdc.consumer;

import java.util.List;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<String, String>> {
   private static final Logger log = LoggerFactory.getLogger(BatchConsumer.class);

   @Override
   public void handleBatch(List<ChangeEvent<String, String>> batch, DebeziumEngine.RecordCommitter<ChangeEvent<String, String>> recordCommitter) throws InterruptedException {
      log.info("Processing {} entries", batch.size());

      try {
         for (ChangeEvent<String, String> ev : batch) {
            process(ev);
            recordCommitter.markProcessed(ev);
         }
      } finally {
         recordCommitter.markBatchFinished();
      }
   }

   private void process(ChangeEvent<String, String> event) {
      log.info("Received event...");
      log.info("KEY -> {}", event.key());
      log.info("VALUE -> {}", event.value());
   }
}
