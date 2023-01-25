package io.gingersnapproject.cdc;

import static io.gingersnapproject.util.Utils.eventually;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import io.gingersnapproject.cdc.cache.CacheIdentifier;
import io.gingersnapproject.cdc.cache.CacheService;
import io.gingersnapproject.cdc.configuration.Rule;
import io.gingersnapproject.cdc.event.Events;
import io.gingersnapproject.cdc.event.NotificationManager;
import io.gingersnapproject.testcontainers.annotation.WithDatabase;
import io.gingersnapproject.util.ArcUtil;
import io.gingersnapproject.util.ByRef;
import io.gingersnapproject.util.Utils;

import io.quarkus.test.junit.QuarkusMock;
import io.quarkus.test.junit.QuarkusTest;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
@WithDatabase(rules = MultiCacheManagerTest.RULE_NAME)
public class MultiCacheManagerTest {
   static final String RULE_NAME = "multimanager";

   private static final NotificationManager notification = mock(NotificationManager.class);

   @Inject ManagedEngine manager;
   @ConfigProperty(name = "gingersnap.cache.uri") URI hotRodURI;

   @BeforeAll
   static void beforeAll() {
      QuarkusMock.installMockForType(notification, NotificationManager.class);
   }

   @BeforeEach
   void beforeEach() {
      clearInvocations(notification);
      reset(notification);
   }

   @Test
   public void testLeaveAndReturn() throws Exception {
      URI additionalMember = createAnotherHotRodURI();
      var additionalId = CacheIdentifier.of(RULE_NAME, additionalMember);
      manager.memberJoined(new Events.CacheMemberJoinEvent(additionalMember));

      // Additional member backend started first time.
      verify(notification, times(1)).backendStartedEvent(eq(additionalId), eq(false));
      eventually(() -> "Additional member engine not started", () -> {
         try {
            verify(notification, times(1)).connectorStarted(eq(additionalId), any());
            return true;
         } catch (Throwable ignore) {
            return false;
         }
      }, 5, TimeUnit.SECONDS);

      Set<URI> knownMembers = Utils.extractField(ManagedEngine.class, "knownMembers", manager);
      assertEquals(2, knownMembers.size());

      manager.memberLeft(new Events.CacheMemberLeaveEvent(additionalMember));
      knownMembers = Utils.extractField(ManagedEngine.class, "knownMembers", manager);
      assertEquals(1, knownMembers.size());
      verify(notification, times(1)).connectorStopped(eq(additionalId));
      verify(notification, times(1)).backendStoppedEvent(eq(additionalId));

      // Join again, should remove from offline list and reconnect the engine.
      manager.memberJoined(new Events.CacheMemberJoinEvent(additionalMember));

      knownMembers = Utils.extractField(ManagedEngine.class, "knownMembers", manager);
      assertEquals(2, knownMembers.size());

      // Additional member backend restarted.
      verify(notification, times(1)).backendStartedEvent(eq(additionalId), eq(true));

      Map<CacheIdentifier, ManagedEngine.StartStopEngine> engines = Utils.extractField(ManagedEngine.class, "engines", manager);
      assertEquals(2, engines.size());
      assertTrue(engines.containsKey(additionalId));
      for (CacheIdentifier identifier : engines.keySet()) {
         assertTrue(identifier.uri().equals(hotRodURI) || identifier.uri().equals(additionalMember));
      }

      eventually(() -> "Additional member engine not started: " + additionalId, () -> {
         try {
            verify(notification, times(1)).connectorStarted(eq(additionalId), any());
            return true;
         } catch (Throwable ignore) {
            return false;
         }
      }, 5, TimeUnit.SECONDS);
      eventually(() -> "Engine not restarted!", () -> {
         var engine = engines.get(additionalId);
         ManagedEngine.Status status = Utils.extractField(engine, "status");
         return status == ManagedEngine.Status.RUNNING;
      }, 15, TimeUnit.SECONDS);
      verify(notification, never()).connectorFailed(any(), any());
   }

   @Test
   public void testRemovingRule() throws Exception {
      URI additionalMember = createAnotherHotRodURI();

      // An additional member joins, so we have 2 engines.
      manager.memberJoined(new Events.CacheMemberJoinEvent(additionalMember));

      manager.removeRule(RULE_NAME);

      // Members are still known.
      Set<URI> knownMembers = Utils.extractField(ManagedEngine.class, "knownMembers", manager);
      assertEquals(2, knownMembers.size());

      // We don't have any engine running and no registered rules.
      Map<CacheIdentifier, ManagedEngine.StartStopEngine> engines = Utils.extractField(ManagedEngine.class, "engines", manager);
      assertTrue(engines.isEmpty());

      Map<CacheIdentifier, Rule> knownRules = Utils.extractField(ManagedEngine.class, "knownRules", manager);
      assertTrue(knownRules.isEmpty());
      verify(notification, never()).connectorFailed(any(), any());
   }

   @Test
   public void testUnhealthyJoinerDoesNotBlockLeaving() throws Exception {
      var throwable = new ByRef<Throwable>(new RuntimeException("Expected failure"));
      Executor executor = Executors.newSingleThreadExecutor();
      CacheService cacheService = ArcUtil.instance(CacheService.class);
      CacheService mockCacheService = spy(cacheService);
      QuarkusMock.installMockForInstance(mockCacheService, cacheService);
      CountDownLatch latch = new CountDownLatch(1);
      CacheIdentifier identifier = CacheIdentifier.of(RULE_NAME, createAnotherHotRodURI());

      doAnswer(invocation -> {
         if (throwable.ref() != null) throw throwable.ref();
         return invocation.callRealMethod();
      }).when(mockCacheService).start(eq(identifier), any());

      // Flaky?
      executor.execute(() -> manager.memberJoined(new Events.CacheMemberJoinEvent(identifier.uri())));
      executor.execute(() -> {
         manager.memberLeft(new Events.CacheMemberLeaveEvent(identifier.uri()));
         latch.countDown();
      });
      assertTrue(latch.await(10, TimeUnit.SECONDS));
      throwable.setRef(null);

      Set<URI> knownMembers = Utils.extractField(ManagedEngine.class, "knownMembers", manager);
      assertEquals(1, knownMembers.size());

      Map<CacheIdentifier, ManagedEngine.StartStopEngine> engines = Utils.extractField(ManagedEngine.class, "engines", manager);
      assertFalse(engines.containsKey(identifier));
   }

   private URI createAnotherHotRodURI() throws URISyntaxException {
      return new URI("hotrod", hotRodURI.getUserInfo(), "127.0.0.1", hotRodURI.getPort(), hotRodURI.getPath(), hotRodURI.getQuery(), hotRodURI.getFragment());
   }
}

