package io.gingersnapproject.cdc.cache.hotrod;

import static io.gingersnapproject.util.Utils.eventually;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import io.gingersnapproject.cdc.cache.CacheIdentifier;
import io.gingersnapproject.cdc.cache.CacheService;
import io.gingersnapproject.cdc.configuration.Rule;
import io.gingersnapproject.cdc.event.NotificationManager;
import io.gingersnapproject.proto.api.config.v1alpha1.KeyFormat;
import io.gingersnapproject.testcontainers.annotation.WithDatabase;
import io.gingersnapproject.util.Utils;

import io.quarkus.test.junit.QuarkusMock;
import io.quarkus.test.junit.QuarkusTest;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@QuarkusTest
@WithDatabase
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class HotRodServiceTest {

   private final NotificationManager eventing = mock(NotificationManager.class);
   private final Rule dummyRule = mock(Rule.class);

   @Inject CacheService service;

   @ConfigProperty(name = "gingersnap.cache.uri") URI hotRodURI;

   @BeforeAll
   public void beforeAll() {
      QuarkusMock.installMockForType(eventing, NotificationManager.class);

      when(dummyRule.valueColumns()).thenReturn(Optional.empty());
      when(dummyRule.keyType()).thenReturn(KeyFormat.TEXT);
      when(dummyRule.keyColumns()).thenReturn(List.of("id"));
      when(dummyRule.plainSeparator()).thenReturn("|");
   }

   @BeforeEach
   public void beforeEach() {
      clearInvocations(eventing);
      reset(eventing);
      // Leak TCP connections.
      ((Map<?, ?>) Utils.extractField(HotRodService.class, "managers", service)).clear();
   }

   @Test
   public void testCacheReconnection() throws Exception {
      var identifier = CacheIdentifier.of("rule", hotRodURI);

      assertNotNull(service.start(identifier, dummyRule));

      verify(eventing, times(1)).backendStartedEvent(eq(identifier), eq(false));

      ConcurrentHashMap<URI, HotRodService.HotRodCache> managers = Utils.extractField(HotRodService.class, "managers", service);
      assertTrue(managers.containsKey(identifier.uri()));
      assertEquals(1, managers.size());

      var manager = managers.get(identifier.uri());
      assertTrue(manager.backends.containsKey(identifier.rule()));
      assertEquals(1, manager.backends.size());

      // We stop the cache.
      service.stop(identifier);

      verify(eventing, times(1)).backendStoppedEvent(eq(identifier));
      assertFalse(manager.backends.get(identifier.rule()).isRunning());

      // Cache manager stops asynchronously.
      eventually(() -> "Cache manager didn't stop", () -> !manager.rcm.isStarted(), 10, TimeUnit.SECONDS);

      // Now the cache restarts.
      service.start(identifier, dummyRule);

      verify(eventing, times(1)).backendStartedEvent(eq(identifier), eq(true));
      verifyNoMoreInteractions(eventing);
      assertTrue(manager.rcm.isStarted());
      assertTrue(managers.get(identifier.uri()).backends.get(identifier.rule()).isRunning());
   }

   @Test
   public void testMultiRulesSameURI() throws Exception {
      var rule1 = CacheIdentifier.of("rule1", hotRodURI);
      var rule2 = CacheIdentifier.of("rule2", hotRodURI);

      var backend1 = service.start(rule1, dummyRule);
      var backend2 = service.start(rule2, dummyRule);

      assertNotSame(backend1, backend2);
      assertTrue(backend1.isRunning());
      assertTrue(backend2.isRunning());

      verify(eventing, times(1)).backendStartedEvent(eq(rule1), eq(false));
      verify(eventing, times(1)).backendStartedEvent(eq(rule2), eq(false));

      ConcurrentHashMap<URI, HotRodService.HotRodCache> managers = Utils.extractField(HotRodService.class, "managers", service);
      assertTrue(managers.containsKey(hotRodURI));
      assertEquals(1, managers.size());

      var manager = managers.get(hotRodURI);
      assertTrue(manager.backends.containsKey(rule1.rule()));
      assertTrue(manager.backends.containsKey(rule2.rule()));
      assertEquals(2, manager.backends.size());

      // Stopping rule2 still keep rule1.
      service.stop(rule2);

      verify(eventing, times(1)).backendStoppedEvent(eq(rule2));
      assertTrue(manager.backends.get(rule1.rule()).isRunning());
      assertFalse(manager.backends.get(rule2.rule()).isRunning());
      assertTrue(manager.rcm.isStarted());

      // Restarting rule2 does not affect rule1.
      service.start(rule2, dummyRule);

      verify(eventing, times(1)).backendStartedEvent(eq(rule2), eq(true));
      verifyNoMoreInteractions(eventing);

      manager = managers.get(hotRodURI);
      assertTrue(manager.backends.get(rule1.rule()).isRunning());
      assertTrue(manager.backends.get(rule2.rule()).isRunning());
      assertNotSame(backend2, manager.backends.get(rule2.rule()));
      assertSame(backend1, manager.backends.get(rule1.rule()));
   }

   @Test
   public void testSameRuleDifferentURIs() throws Exception {
      URI anotherURI = createAnotherHotRodURI();
      var ruleA = CacheIdentifier.of("rule", hotRodURI);
      var ruleB = CacheIdentifier.of("rule", anotherURI);

      var backend1 = service.start(ruleA, dummyRule);
      var backend2 = service.start(ruleB, dummyRule);

      assertNotSame(backend1, backend2);
      assertTrue(backend1.isRunning());
      assertTrue(backend2.isRunning());

      verify(eventing, times(1)).backendStartedEvent(eq(ruleA), eq(false));
      verify(eventing, times(1)).backendStartedEvent(eq(ruleB), eq(false));

      ConcurrentHashMap<URI, HotRodService.HotRodCache> managers = Utils.extractField(HotRodService.class, "managers", service);
      assertTrue(managers.containsKey(hotRodURI));
      assertTrue(managers.containsKey(anotherURI));
      assertEquals(2, managers.size());

      // Stopping one URI does not affect the other.
      service.stop(ruleB);

      verify(eventing, times(1)).backendStoppedEvent(eq(ruleB));

      var managerA = managers.get(hotRodURI);
      var managerB = managers.get(anotherURI);

      assertTrue(managerA.rcm.isStarted());
      eventually(() -> "Cache manager for another rule didn't stop", () -> !managerB.rcm.isStarted(), 10, TimeUnit.SECONDS);

      // Restarting ruleB does not affect ruleA.
      service.start(ruleB, dummyRule);

      verify(eventing, times(1)).backendStartedEvent(eq(ruleB), eq(true));
      verifyNoMoreInteractions(eventing);

      assertSame(managerA, managers.get(hotRodURI));
      assertNotSame(managerB, managers.get(anotherURI));

      assertTrue(managerA.rcm.isStarted());
      assertTrue(managers.get(anotherURI).rcm.isStarted());
   }

   private URI createAnotherHotRodURI() throws URISyntaxException {
      return new URI("hotrod", hotRodURI.getUserInfo(), "127.0.0.1", hotRodURI.getPort(), hotRodURI.getPath(), hotRodURI.getQuery(), hotRodURI.getFragment());
   }
}
