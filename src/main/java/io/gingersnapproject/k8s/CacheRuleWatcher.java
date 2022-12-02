package io.gingersnapproject.k8s;

import java.util.Map;
import java.util.Map.Entry;

import javax.enterprise.event.Observes;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import javax.inject.Singleton;

import io.gingersnapproject.k8s.configuration.KubernetesConfiguration;
import io.gingersnapproject.proto.api.config.v1alpha1.EagerCacheRuleSpec;
import io.gingersnapproject.proto.api.config.v1alpha1.EagerCacheRuleSpec.Builder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;

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
      private interface SendEventFunc{
         public void sendEvent(String name, EagerCacheRuleSpec rule);
      }
      private void processConfigMapAndSend(ConfigMap cm, SendEventFunc f) {
         Map<String, String> rulesMap = cm.getData();
         for (Entry<String, String> entry : rulesMap.entrySet()) {
            Builder eRuleBuilder = EagerCacheRuleSpec.newBuilder();
            try {
               JsonFormat.parser().ignoringUnknownFields().merge(entry.getValue(), eRuleBuilder);
            } catch (InvalidProtocolBufferException e) {
               log.error("Cannot parse eager rule with name {}", entry.getKey());
               log.debug("{}",e);
            }
            f.sendEvent(entry.getKey(), eRuleBuilder.build());
         }
      }

      @Override
      public void eventReceived(Action action, ConfigMap resource) {
         log.info("Watcher received event: [{}] -> {}", action, resource);
         switch (action) {
            case ADDED:
               processConfigMapAndSend(resource, (name,rule) -> {log.info("calling DynamicRuleManagement.addRule({},{}", name, rule.toString());});
               break;
            case MODIFIED:
               processConfigMapAndSend(resource, (name,rule) -> {log.info("calling DynamicRuleManagement.changeRule({},{}", name, rule.toString());});
               break;
            case DELETED:
               processConfigMapAndSend(resource, (name,rule) -> {log.info("calling DynamicRuleManagement.deleteRule({},{}", name, rule.toString());});
               break;
            case BOOKMARK:
               break;
            case ERROR:
               break;
            default:
               break;

         }
      }

      @Override
      public void onClose(WatcherException cause) {
         log.error("Closing watcher", cause);
      }
   }
}
