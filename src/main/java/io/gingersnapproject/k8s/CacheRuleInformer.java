package io.gingersnapproject.k8s;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import javax.enterprise.event.Observes;
import javax.enterprise.inject.Instance;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import javax.inject.Singleton;

import io.gingersnapproject.cdc.configuration.Rule;
import io.gingersnapproject.k8s.configuration.KubernetesConfiguration;
import io.gingersnapproject.proto.api.config.v1alpha1.EagerCacheRuleSpec;
import io.gingersnapproject.proto.api.config.v1alpha1.EagerCacheRuleSpec.Builder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;

@Singleton
public class CacheRuleInformer {

   private static final Logger log = LoggerFactory.getLogger(CacheRuleInformer.class);

   @Inject Instance<KubernetesClient> client;
   @Inject
   KubernetesConfiguration configuration;
   @Inject
   EagerCacheRuleSpecAdapterProducer singleRuleProducer;
   private SharedIndexInformer<ConfigMap> informer;

   void startWatching(@Observes StartupEvent ignore) {
      if (client.isUnsatisfied() || configuration.configMapName().isEmpty()) {
         log.info("Kubernetes client not found, not watching config map");
         return;
      }

      String ruleName = configuration.configMapName().get();
      log.info("Informer on rules in {}", ruleName);
      final long RESYNC_PERIOD = 60 * 1000L;
      KubernetesClient kc = client.get();
      informer = kc.configMaps().withName(ruleName).inform(new ConfigMapEventHandler(), RESYNC_PERIOD); 
      informer.start();
   }

   public void stop(@Observes ShutdownEvent ignore) {
      if (informer != null) {
         log.info("Shutdown informer");
         informer.close();
      }
   }

   private static final class ConfigMapEventHandler implements ResourceEventHandler<ConfigMap> {
      private interface SendEventFunc{
         void sendEvent(String name, EagerCacheRuleSpec rule);
      }
      private void processConfigMapAndSend(Set<Map.Entry<String,String>> entries, SendEventFunc f) {
         for (Entry<String, String> entry : entries) {
            Builder eRuleBuilder = EagerCacheRuleSpec.newBuilder();
            try {
               JsonFormat.parser().ignoringUnknownFields().merge(entry.getValue(), eRuleBuilder);
            } catch (InvalidProtocolBufferException e) {
               log.error("Cannot parse eager rule with name {}", entry.getKey(), e);
               log.debug("{}",e);
            }
            f.sendEvent(entry.getKey(), eRuleBuilder.build());
         }
      }

      @Override
      public void onAdd(ConfigMap obj) {
         processConfigMapAndSend(obj.getData().entrySet(), (name,rule) -> {log.info("calling DynamicRuleManagement.addRule({},{}", name, rule.toString());});         
      }

      @Override
      public void onUpdate(ConfigMap oldObj, ConfigMap newObj) {
         Map<String, String> oldMap =  oldObj.getData();
         Map<String, String> newMap = newObj.getData();
         Set<Map.Entry<String,String>> olds = oldMap.entrySet();
         Set<Map.Entry<String,String>> news = newMap.entrySet();
         Set<Map.Entry<String,String>> added,removed,changed;
         added = new HashSet<Map.Entry<String,String>>(news);
         // Get added keys. From new set remove
         // - keys already present in old set
         added.removeIf(arg0 -> {return oldMap.containsKey(arg0.getKey()); });

         removed = new HashSet<Map.Entry<String,String>>(olds);
         // Get removed keys. From old set remove
         // - keys still present in new set
         removed.removeIf((arg0 -> {return newMap.containsKey(arg0.getKey()); }));

         changed = new HashSet<Map.Entry<String,String>>(olds);
         // Get the changed keys. From the old set remove
         // - keys which aren't in the new set (deleted)
         // - keys which are in the new set with same value (unchanged)
         changed.removeIf(arg0 -> !newMap.containsKey(arg0.getKey()) || arg0.getValue().equals(newMap.get(arg0.getKey())));

         processConfigMapAndSend(changed, (name,rule) -> {log.info("calling DynamicRuleManagement.updateRule({},{}", name, rule.toString());});
         processConfigMapAndSend(removed, (name,rule) -> {log.info("calling DynamicRuleManagement.removeRule({},{}", name, rule.toString());});
         processConfigMapAndSend(added, (name,rule) -> {log.info("calling DynamicRuleManagement.addRule({},{}", name, rule.toString());});
      }

      @Override
      public void onDelete(ConfigMap obj, boolean deletedFinalStateUnknown) {
         processConfigMapAndSend(obj.getData().entrySet(), (name,rule) -> {log.info("calling DynamicRuleManagement.deleteRule({},{}", name, rule.toString());});
      }
   }
}

// Used only by CacheRuleInformer
class EagerCacheRuleSpecAdapter implements Rule.SingleRule
{
   public EagerCacheRuleSpecAdapter(Configuration config) {
   }

    @Override
    public Connector connector() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Database database() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Backend backend() {
        // TODO Auto-generated method stub
        return null;
    }
}
