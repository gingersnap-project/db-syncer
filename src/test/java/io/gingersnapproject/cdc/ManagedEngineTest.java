package io.gingersnapproject.cdc;

import static io.gingersnapproject.util.Utils.eventually;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import io.gingersnapproject.cdc.cache.CacheIdentifier;
import io.gingersnapproject.cdc.cache.CacheService;
import io.gingersnapproject.cdc.configuration.Cache;
import io.gingersnapproject.cdc.configuration.Configuration;
import io.gingersnapproject.cdc.configuration.Connector;
import io.gingersnapproject.cdc.configuration.Database;
import io.gingersnapproject.cdc.configuration.Rule;
import io.gingersnapproject.cdc.connector.DatabaseProvider;
import io.gingersnapproject.cdc.event.Events;
import io.gingersnapproject.cdc.event.NotificationManager;
import io.gingersnapproject.proto.api.config.v1alpha1.KeyFormat;
import io.gingersnapproject.util.Utils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ManagedEngineTest {

   private final Configuration configurationMock = mock(Configuration.class);
   private final CacheService cacheServiceMock = mock(CacheService.class);
   private final NotificationManager notificationManagerMock = mock(NotificationManager.class);

   private ManagedEngine managedEngine;

   @BeforeEach
   public void initializeMocks() {
      reset(configurationMock, cacheServiceMock, notificationManagerMock);
      clearInvocations(configurationMock, cacheServiceMock, notificationManagerMock);

      when(configurationMock.rules()).thenReturn(Map.of("rule", new MockTestRule()));
      when(configurationMock.database()).thenReturn(new MockDatabase());
      when(configurationMock.cache()).thenReturn(new MockCache());

      managedEngine = new ManagedEngine();
      managedEngine.config = configurationMock;
      managedEngine.cacheService = cacheServiceMock;
      managedEngine.eventing = notificationManagerMock;
   }

   @Test
   public void testStartupAndShutdownEvents() throws Exception {
      var identifier = CacheIdentifier.of("rule", URI.create("hotrod://localhost:11222"));
      when(cacheServiceMock.start(any(), any())).thenReturn(mock(CacheBackend.class));

      managedEngine.start(null);
      managedEngine.memberJoined(new Events.CacheMemberJoinEvent(identifier.uri()));

      verify(cacheServiceMock, times(1)).start(eq(identifier), any());

      Map<String, Rule> knownRules = Utils.extractField(managedEngine, "knownRules");
      assertEquals(knownRules.size(), 1);
      assertTrue(knownRules.containsKey("rule"));

      Map<CacheIdentifier, ManagedEngine.StartStopEngine> engines = Utils.extractField(ManagedEngine.class, "engines", managedEngine);

      assertEquals(engines.size(), 1);
      assertTrue(engines.containsKey(identifier));

      var sse = engines.get(identifier);
      ManagedEngine.Status status = Utils.extractField(ManagedEngine.StartStopEngine.class, "status", sse);
      assertEquals(ManagedEngine.Status.RUNNING, status);

      // Check we have an event for the connector, since the mock information is invalid we should notify this.
      BooleanSupplier checkInteraction = () -> {
         try {
            verify(notificationManagerMock).connectorFailed(any(), any());
         } catch (Throwable ignore) {
            return false;
         }
         return true;
      };
      eventually(() -> "No interactions with notifications", checkInteraction, 5, TimeUnit.SECONDS);

      // Now the engines are shutdown
      managedEngine.stop(null);
      BooleanSupplier bs = () -> {
         ManagedEngine.Status s = Utils.extractField(ManagedEngine.StartStopEngine.class, "status", sse);
         return s == ManagedEngine.Status.SHUTDOWN;
      };
      eventually(() -> "Engine did not shutdown", bs, 5, TimeUnit.SECONDS);
   }

   @Test
   public void testEngineTriesToReconnectOnFailure() throws Exception {
      var identifier = CacheIdentifier.of("rule", URI.create("hotrod://localhost:11222"));
      when(cacheServiceMock.start(any(), any())).thenReturn(mock(CacheBackend.class));

      managedEngine.start(null);
      managedEngine.memberJoined(new Events.CacheMemberJoinEvent(identifier.uri()));

      verify(cacheServiceMock, times(1)).start(eq(identifier), any());

      Map<String, Rule> knownRules = Utils.extractField(managedEngine, "knownRules");
      assertEquals(knownRules.size(), 1);
      assertTrue(knownRules.containsKey("rule"));

      Map<CacheIdentifier, ManagedEngine.StartStopEngine> engines = Utils.extractField(ManagedEngine.class, "engines", managedEngine);

      assertEquals(1, engines.size());
      assertTrue(engines.containsKey(identifier));

      var sse = engines.get(identifier);
      ManagedEngine.Status status = Utils.extractField(ManagedEngine.StartStopEngine.class, "status", sse);

      // Engine still on running as it didn't receive the failure event.
      assertEquals(ManagedEngine.Status.RUNNING, status);

      managedEngine.engineFailed(new Events.ConnectorFailedEvent(identifier, null));
      BooleanSupplier bs = () -> {
         ManagedEngine.Status s = Utils.extractField(ManagedEngine.StartStopEngine.class, "status", sse);
         return s == ManagedEngine.Status.RETRYING;
      };
      eventually(() -> "Engine did not enter into retry", bs, 10, TimeUnit.SECONDS);
   }

   @Test
   public void testEnginesCoExistOnBackendFailure() throws Exception {
      var rule1Id = CacheIdentifier.of("rule-1", URI.create("hotrod://10.0.0.1:11222"));
      var rule2Id = CacheIdentifier.of("rule-2", URI.create("hotrod://10.0.0.2:11222"));
      when(configurationMock.rules()).thenReturn(Map.of("rule-1", new MockTestRule(), "rule-2", new MockTestRule()));

      managedEngine.start(null);

      managedEngine.memberJoined(new Events.CacheMemberJoinEvent(rule1Id.uri()));
      managedEngine.memberJoined(new Events.CacheMemberJoinEvent(rule2Id.uri()));

      verify(cacheServiceMock, times(1)).start(eq(rule1Id), any());
      verify(cacheServiceMock, times(1)).start(eq(rule2Id), any());

      Map<String, Rule> knownRules = Utils.extractField(managedEngine, "knownRules");
      assertEquals(knownRules.size(), 2);
      assertTrue(knownRules.containsKey("rule-1"));
      assertTrue(knownRules.containsKey("rule-2"));

      Map<CacheIdentifier, ManagedEngine.StartStopEngine> engines = Utils.extractField(ManagedEngine.class, "engines", managedEngine);
      // nbr of rules * nbr of members
      assertEquals(4, engines.size());

      // Assert all engines are in running mode.
      for (var sse : engines.values()) {
         ManagedEngine.Status status = Utils.extractField(ManagedEngine.StartStopEngine.class, "status", sse);
         assertEquals(ManagedEngine.Status.RUNNING, status);
      }

      // Engine from `rule-1` fails, but `rule-2` continues to work.
      managedEngine.backendFailed(new Events.BackendFailedEvent(rule1Id, null));

      // After backend failure, engines will stop and enter retry mode.
      for (var entry : engines.entrySet()) {
         var sse = entry.getValue();
         if (rule1Id.equals(entry.getKey())) {
            BooleanSupplier bs = () -> {
               ManagedEngine.Status s = Utils.extractField(ManagedEngine.StartStopEngine.class, "status", sse);
               System.out.println(s);
               return s == ManagedEngine.Status.RETRYING;
            };
            eventually(() -> String.format("Engine '%s' did not enter into retry", entry.getKey()), bs, 10, TimeUnit.SECONDS);
         } else {
            ManagedEngine.Status status = Utils.extractField(ManagedEngine.StartStopEngine.class, "status", sse);
            assertEquals(ManagedEngine.Status.RUNNING, status);
         }
      }

      verify(cacheServiceMock, atMost(4)).stop(any());
   }

   @Test
   public void testAddingAndRemovingRule() throws Exception {
      var identifier = CacheIdentifier.of("rule", URI.create("hotrod://localhost:11222"));
      managedEngine.addRule("rule", new MockTestRule());
      managedEngine.memberJoined(new Events.CacheMemberJoinEvent(identifier.uri()));

      verify(cacheServiceMock, times(1)).start(eq(identifier), any());

      Map<String, Rule> knownRules = Utils.extractField(managedEngine, "knownRules");
      assertEquals(knownRules.size(), 1);
      assertTrue(knownRules.containsKey(identifier.rule()));

      Map<CacheIdentifier, ManagedEngine.StartStopEngine> engines = Utils.extractField(ManagedEngine.class, "engines", managedEngine);

      assertEquals(engines.size(), 1);
      assertTrue(engines.containsKey(identifier));

      var sse = engines.get(identifier);
      ManagedEngine.Status status = Utils.extractField(ManagedEngine.StartStopEngine.class, "status", sse);
      assertEquals(ManagedEngine.Status.RUNNING, status);

      // Now we can remove the added rule. It will shutdown the engine and remove from the list.
      managedEngine.removeRule(identifier.rule());

      BooleanSupplier bs = () -> {
         ManagedEngine.Status s = Utils.extractField(ManagedEngine.StartStopEngine.class, "status", sse);
         return s == ManagedEngine.Status.SHUTDOWN;
      };
      eventually(() -> "Engine did not shutdown", bs, 5, TimeUnit.SECONDS);
      assertFalse(engines.containsKey(identifier));
      assertFalse(knownRules.containsKey(identifier.rule()));
   }

   private static final class MockCache implements Cache {

      @Override
      public URI uri() {
         return URI.create("hotrod://localhost:11222");
      }
   }

   private static final class MockDatabase implements Database {

      @Override
      public DatabaseProvider type() {
         return DatabaseProvider.MYSQL;
      }

      @Override
      public String host() {
         return "localhost";
      }

      @Override
      public int port() {
         return 0;
      }

      @Override
      public String username() {
         return "test";
      }

      @Override
      public String password() {
         return "test";
      }

      @Override
      public Optional<String> database() {
         return Optional.empty();
      }
   }

   private static final class MockTestRule implements Rule {
      @Override
      public Connector connector() {
         return new Connector() {
            @Override
            public String schema() {
               return "testing";
            }

            @Override
            public String table() {
               return "testers";
            }
         };
      }

      @Override
      public KeyFormat keyType() {
         return KeyFormat.TEXT;
      }

      @Override
      public String plainSeparator() {
         return "|";
      }

      @Override
      public List<String> keyColumns() {
         return List.of("id");
      }

      @Override
      public Optional<List<String>> valueColumns() {
         return Optional.empty();
      }
   }
}
