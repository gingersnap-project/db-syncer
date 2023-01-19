package io.gingersnapproject.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.InetAddress;
import java.net.URI;
import java.util.HashSet;
import java.util.List;

import io.gingersnapproject.cdc.configuration.Cache;
import io.gingersnapproject.cdc.configuration.Configuration;
import io.gingersnapproject.cdc.event.NotificationManager;
import io.gingersnapproject.util.Utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class MembershipListenerTest {

   private final Configuration configurationMock = mock(Configuration.class);
   private final NotificationManager notificationMock = mock(NotificationManager.class);
   private MembershipListener listener;

   @BeforeEach
   public void beforeEach() {
      clearInvocations(configurationMock);
      reset(configurationMock);

      listener = new MembershipListener();
      listener.configuration = configurationMock;
      listener.notificationManager = notificationMock;
   }


   @Test
   public void testMembershipOperations() throws Exception {
      InetAddress m1 = InetAddress.getByName("10.0.0.10"),
            m2 = InetAddress.getByName("10.0.0.11"),
            m3 = InetAddress.getByName("10.0.0.12");
      List<InetAddress> addresses = List.of(m1, m2, m3);

      listener = spy(listener);

      var cacheConf = mock(Cache.class);
      when(cacheConf.uri()).thenReturn(URI.create("hotrod://admin:password@cache-manager:11222?sasl_mechanism=SCRAM-SHA-512"));
      when(configurationMock.cache()).thenReturn(cacheConf);
      when(listener.resolve("cache-manager")).thenReturn(addresses);

      listener.pollDNSInformation();

      // The members are added.
      Assertions.assertEquals(new HashSet<>(addresses), Utils.extractField(MembershipListener.class, "members", listener));
      verify(notificationMock, times(3)).memberJoinEvent(any());
      verify(notificationMock, times(1)).memberJoinEvent(eq(URI.create("hotrod://admin:password@10.0.0.10:11222?sasl_mechanism=SCRAM-SHA-512")));

      // Member m3 left.
      addresses = List.of(m1, m2);
      when(listener.resolve("cache-manager")).thenReturn(addresses);
      listener.pollDNSInformation();
      Assertions.assertEquals(new HashSet<>(addresses), Utils.extractField(MembershipListener.class, "members", listener));
      verify(notificationMock, times(1)).memberLeaveEvent(eq(URI.create("hotrod://admin:password@10.0.0.12:11222?sasl_mechanism=SCRAM-SHA-512")));

      // Member m3 joins again.
      addresses = List.of(m1, m2, m3);
      when(listener.resolve("cache-manager")).thenReturn(addresses);
      listener.pollDNSInformation();
      Assertions.assertEquals(new HashSet<>(addresses), Utils.extractField(MembershipListener.class, "members", listener));
      verify(notificationMock, times(4)).memberJoinEvent(any());
   }
}
