package io.gingersnapproject.k8s;

import javax.enterprise.event.Observes;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import javax.inject.Singleton;

import io.gingersnapproject.k8s.configuration.KubernetesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.WatcherException;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;

@Singleton
public class CacheRuleWatcher {

   private static final Logger log = LoggerFactory.getLogger(CacheRuleWatcher.class);

   @Inject Instance<KubernetesClient> client;
   @Inject
   KubernetesConfiguration configuration;
   private Watch watcher;

   void startWatching(@Observes StartupEvent ignore) {
      if (client.isUnsatisfied() || configuration.configMapName().isEmpty()) {
         log.info("Kubernetes client not found, not watching config map");
         return;
      }

      String ruleName = configuration.configMapName().get();
      log.info("Watching for rules on {}", ruleName);

      KubernetesClient kc = client.get();
      watcher = kc.configMaps().withName(ruleName)
            .watch(new ConfigMapWatcher());
   }

   public void stop(@Observes ShutdownEvent ignore) {
      if (watcher != null) {
         log.info("Shutdown watcher");
         watcher.close();
      }
   }

   private static final class ConfigMapWatcher implements Watcher<ConfigMap> {

      @Override
      public void eventReceived(Action action, ConfigMap resource) {
         log.info("Watcher received event: [{}] -> {}", action, resource);
      }

      @Override
      public void onClose(WatcherException cause) {
         log.error("Closing watcher", cause);
      }
   }
}
