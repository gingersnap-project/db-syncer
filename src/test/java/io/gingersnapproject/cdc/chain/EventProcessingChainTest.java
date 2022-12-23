package io.gingersnapproject.cdc.chain;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.clearInvocations;

import java.util.UUID;

import io.gingersnapproject.cdc.configuration.Connector;
import io.gingersnapproject.cdc.configuration.Rule;
import io.gingersnapproject.fwk.ControlledCacheBackend;

import org.infinispan.commons.dataconversion.internal.Json;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.Mockito;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class EventProcessingChainTest {
   private static final String TABLE_NAME = "customer";
   private static final String KEY = "key";
   private final ControlledCacheBackend backend = new ControlledCacheBackend(j -> j.at(KEY));
   private final Rule customerRule = Mockito.mock(Rule.class);

   @BeforeAll
   public void createMocks() {
      Connector connector = Mockito.mock(Connector.class);
      Mockito.when(customerRule.connector()).thenReturn(connector);
      Mockito.when(connector.table()).thenReturn(TABLE_NAME);
   }

   @BeforeEach
   public void init() {
      clearInvocations(customerRule);
      backend.clear();
   }

   @Test
   public void testEventIsPublished() {
      EventProcessingChain chain = EventProcessingChainFactory.create(customerRule, backend);

      Json value = createContent();

      assertTrue(chain.process(new Event(Json.object(), createValue("c", value)), null));
      assertTrue(backend.data().contains(value));
      assertEquals(1, backend.data().size());

      value.set("added_later", UUID.randomUUID().toString());
      assertTrue(chain.process(new Event(Json.object(), createValue("u", value)), null));
      assertTrue(backend.data().contains(value));
      assertEquals(1, backend.data().size());

      assertTrue(chain.process(new Event(Json.object(), createValue("d", value)), null));
      assertTrue(backend.data().isEmpty());
   }

   @Test
   public void testEventFiltered() {
      EventProcessingChain chain = EventProcessingChainFactory.create(customerRule, backend);
      Json content = createContent();

      // Filter unregistered tables.
      Json value = createValue("c", content);
      value.at("source").set("table", "other_table");
      assertFalse(chain.process(new Event(Json.object(), value), null));
      assertTrue(backend.data().isEmpty());

      // Filter DDL events.
      value = createValue("c", content);
      value.set("ddl", "CREATE TABLE customer");
      assertFalse(chain.process(new Event(Json.object(), value), null));
      assertTrue(backend.data().isEmpty());

      // Filter if source/table not present.
      value = createValue("c", content);
      value.set("source", null);
      assertFalse(chain.process(new Event(Json.object(), value), null));
      assertTrue(backend.data().isEmpty());

      value = createValue("c", content);
      value.at("source").set("table", null);
      assertFalse(chain.process(new Event(Json.object(), value), null));
      assertTrue(backend.data().isEmpty());
   }

   @Test
   public void testUnknownOperationNotAccepted() {
      EventProcessingChain chain = EventProcessingChainFactory.create(customerRule, backend);
      Json content = createContent();

      Json value = createValue("c", content);
      value.set("op", "what");

      assertFalse(chain.process(new Event(Json.object(), value), null));
      assertTrue(backend.data().isEmpty());
   }

   private static Json createValue(String operation, Json content) {
      Json value = Json.object();
      value.set("source", createSource());

      switch (operation) {
         case "c", "r", "u" -> value.set("after", clone(content));
         case "d" -> value.set("before", clone(content));
         default -> throw new IllegalStateException("Unknown " + operation);
      }

      value.set("op", operation);
      return value;
   }

   private static Json createContent() {
      Json value = Json.object();
      value.set("value", UUID.randomUUID().toString());
      value.set(KEY, UUID.randomUUID().toString());
      return value;
   }

   private static Json createSource() {
      Json source = Json.object();
      source.set("table", TABLE_NAME);
      return source;
   }

   private static Json clone(Json source) {
      return Json.read(source.toString());
   }
}
