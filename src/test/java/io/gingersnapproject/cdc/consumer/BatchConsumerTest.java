package io.gingersnapproject.cdc.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;

import io.gingersnapproject.cdc.EngineWrapper;
import io.gingersnapproject.cdc.chain.Event;
import io.gingersnapproject.cdc.chain.EventProcessingChain;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import org.apache.kafka.connect.source.SourceRecord;
import org.infinispan.commons.dataconversion.internal.Json;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class BatchConsumerTest {

   private final EngineWrapper engineMock = mock(EngineWrapper.class);
   private final EventProcessingChain chainMock = mock(EventProcessingChain.class);
   private BatchConsumer consumer;

   @BeforeAll
   public void initializeMocks() {
      when(chainMock.process(any(), any())).thenReturn(true);
   }

   @BeforeEach
   public void initializeConsumer() {
      reset(engineMock, chainMock);
      clearInvocations(engineMock, chainMock);
      doNothing().when(engineMock).notifyError(any());
      consumer = new BatchConsumer(engineMock, chainMock, Executors.newSingleThreadExecutor());
   }

   @Test
   public void testAllEventPublishedAndCommitted() throws Exception {
      int eventSize = 10;
      var committer = Mockito.mock(DebeziumEngine.RecordCommitter.class);
      var records = new ArrayList<ChangeEvent<SourceRecord, SourceRecord>>();

      doAnswer(ignore -> {
         throw new IllegalStateException("Should not call error");
      }).when(engineMock).notifyError(any());

      for (int i = 0; i < eventSize; i++) {
         records.add(changeEvent());
      }

      consumer.handleBatch(records, committer);

      verify(committer, times(eventSize)).markProcessed(any());
      verify(committer, times(1)).markBatchFinished();
      verify(chainMock, times(eventSize)).process(any(), any());
      verifyNoInteractions(engineMock);
   }

   @Test
   public void testDuplicatedKeysAreSquashed() throws Exception {
      int eventSize = 10;
      var committer = Mockito.mock(DebeziumEngine.RecordCommitter.class);
      var records = new ArrayList<ChangeEvent<SourceRecord, SourceRecord>>();

      doAnswer(ignore -> {
         throw new IllegalStateException("Should not call error");
      }).when(engineMock).notifyError(any());

      var key = Json.object("id", "value");
      var lastEvent = changeEvent(key);

      for (int i = 0; i < eventSize; i++) {
         records.add(changeEvent(key));
      }
      records.add(lastEvent);

      consumer.handleBatch(records, committer);

      // Verify that we published only one event, and it was the last change event.
      var publishedEvent = ArgumentCaptor.forClass(Event.class);
      verify(chainMock, times(1)).process(publishedEvent.capture(), any());
      assertNotNull(publishedEvent.getValue());

      Event captured = publishedEvent.getValue();

      assertEquals(captured.key(), key);
      assertEquals(captured.value(), lastEvent.value().value());

      // Event publishing only the last, we must commit all events in the batch.
      verify(committer, times(eventSize + 1)).markProcessed(any());
      verify(committer, times(1)).markBatchFinished();
      verifyNoInteractions(engineMock);
   }

   @Test
   public void testNotifyExceptionOnFailure() {
      var committer = Mockito.mock(DebeziumEngine.RecordCommitter.class);

      var ex = new RuntimeException("Something feels wrong");
      when(chainMock.process(any(), any())).thenThrow(ex);
      when(engineMock.getName()).thenReturn("engine-name");

      consumer.handleBatch(List.of(changeEvent()), committer);

      verifyNoInteractions(committer);

      var captor = ArgumentCaptor.forClass(Exception.class);
      verify(engineMock, times(1)).notifyError(captor.capture());

      Exception actual = captor.getValue();
      assertTrue(actual instanceof CompletionException);
      assertEquals(ex, actual.getCause());
   }

   private ChangeEvent<SourceRecord, SourceRecord> changeEvent() {
      return changeEvent(createEvent());
   }

   private ChangeEvent<SourceRecord, SourceRecord> changeEvent(Json valueKey) {
      return changeEvent(createEvent(valueKey));
   }

   private ChangeEvent<SourceRecord, SourceRecord> changeEvent(SourceRecord value) {
      return new ChangeEvent<>() {
         @Override
         public SourceRecord key() {
            return null;
         }

         @Override
         public SourceRecord value() {
            return value;
         }

         @Override
         public String destination() {
            return null;
         }
      };
   }

   private SourceRecord createEvent() {
      return createEvent(Json.object("key", UUID.randomUUID().toString()));
   }

   private SourceRecord createEvent(Json key) {
      Json value = Json.object()
            .set("source", Json.object("table", "some_table"))
            .set("before", Json.object("key", "key", "value", "value"))
            .set("after", Json.object("key", "key", "value", "value"))
            .set("op", "c");

      return new SourceRecord(null, null, "topic", null, key, null, value);
   }
}
