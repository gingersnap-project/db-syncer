package io.gingersnapproject.service;

import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;

import io.gingersnapproject.cdc.ReschedulingTask;
import io.gingersnapproject.cdc.configuration.Configuration;
import io.gingersnapproject.cdc.event.NotificationManager;

import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class MembershipListener {

   private static final Logger log = LoggerFactory.getLogger(MembershipListener.class);
   private static final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r,"membership-listener"));

   // Accessed by a single thread.
   private final Set<InetAddress> members = new HashSet<>();
   @Inject NotificationManager notificationManager;
   @Inject Configuration configuration;

   @ConfigProperty(name = "gingersnap.dynamic-membership", defaultValue = "false")
   boolean dynamicMembership;

   private ReschedulingTask<Void> task;
   private volatile boolean shutdown;

   void onStartup(@Observes StartupEvent ignore) {
      if (task != null) return;

      if (dynamicMembership) {
         task = new ReschedulingTask<>(executor, this::pollDNSInformation, __ -> false, 15, TimeUnit.SECONDS, t -> {
            log.debug("Poll DNS failure", t);
            return true;
         });
         executor.submit(() -> {
            task.schedule();
         });
      } else {
         notificationManager.memberJoinEvent(configuration.cache().uri());
      }
   }

   void onShutdown(@Observes ShutdownEvent ignore) {
      if (task == null) return;

      log.info("Shutdown membership listener");
      task.close();
      task = null;
      shutdown = true;
   }

   // package-private for testing only.
   CompletionStage<Void> pollDNSInformation() {
      if (shutdown) return CompletableFutures.completedNull();

      var uri = configuration.cache().uri();
      Set<InetAddress> joiners = new HashSet<>(resolve(uri.getHost()));
      Set<InetAddress> leavers = new HashSet<>(members);

      leavers.removeIf(joiners::contains);
      joiners.removeIf(members::contains);

      members.removeAll(leavers);
      members.addAll(joiners);

      log.debug("Updating membership, joined '{}' and left '{}', current '{}'", joiners, leavers, members);
      for (InetAddress leaver : leavers) {
         notificationManager.memberLeaveEvent(asURI(uri, leaver));
      }

      for (InetAddress joiner : joiners) {
         notificationManager.memberJoinEvent(asURI(uri, joiner));
      }

      return CompletableFutures.completedNull();
   }

   List<InetAddress> resolve(String query) {
      try {
         return Arrays.asList(InetAddress.getAllByName(query));
      } catch (UnknownHostException e) {
         log.error("Failed querying '{}'", query, e);
         return Collections.emptyList();
      }
   }

   private URI asURI(URI configured, InetAddress address) {
      StringBuilder sb = new StringBuilder(configured.getScheme())
            .append("://");

      if (configured.getUserInfo() != null) {
         sb.append(configured.getUserInfo())
               .append("@");
      }

      sb.append(address.getHostAddress());

      if (configured.getPort() > 0) {
         sb.append(":")
               .append(configured.getPort());
      }

      if (configured.getQuery() != null) {
         sb.append("?")
               .append(configured.getQuery());
      }

      return URI.create(sb.toString());
   }
}
