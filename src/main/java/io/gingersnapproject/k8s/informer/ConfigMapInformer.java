package io.gingersnapproject.k8s.informer;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import io.gingersnapproject.cdc.DynamicRuleManagement;
import io.gingersnapproject.cdc.configuration.Connector;
import io.gingersnapproject.cdc.configuration.Rule;
import io.gingersnapproject.k8s.configuration.KubernetesConfiguration;
import io.gingersnapproject.proto.api.config.v1alpha1.EagerCacheRuleSpec;
import io.gingersnapproject.proto.api.config.v1alpha1.KeyFormat;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class ConfigMapInformer implements KubernetesInformer<ConfigMap> {

   private static final Logger log = LoggerFactory.getLogger(ConfigMapInformer.class);

   @Inject
   KubernetesConfiguration configuration;

   @Inject
   DynamicRuleManagement drm;

   public ConfigMapInformer() { }

   // For testing only.
   public ConfigMapInformer(DynamicRuleManagement drm) {
      this.drm = drm;
   }

   private void processConfigMapAndSend(Set<Map.Entry<String, String>> entries, SendEventFunc f) {
      for (Map.Entry<String, String> entry : entries) {
         EagerCacheRuleSpec.Builder eRuleBuilder = EagerCacheRuleSpec.newBuilder();
         try {
            JsonFormat.parser().ignoringUnknownFields().merge(entry.getValue(), eRuleBuilder);
         } catch (InvalidProtocolBufferException e) {
            log.error("Cannot parse eager rule with name {}", entry.getKey(), e);
         }
         f.sendEvent(entry.getKey(), eRuleBuilder.build());
      }
   }

   @Override
   public SharedIndexInformer<ConfigMap> register(KubernetesClient kc) {
      String ruleName = configuration.configMapName().get();
      return kc.configMaps().withName(ruleName).inform(this, 60 * 1000);
   }

   @Override
   public void onAdd(ConfigMap obj) {
      processConfigMapAndSend(obj.getData().entrySet(), (name, rule) -> {
         log.debug("calling DynamicRuleManagement.addRule({},{}", name, rule.toString());
         drm.addRule(name, new EagerCacheRuleSpecAdapter(rule));
      });
   }

   @Override
   public void onUpdate(ConfigMap oldObj, ConfigMap newObj) {
      Map<String, String> oldMap = oldObj.getData();
      Map<String, String> newMap = newObj.getData();
      var olds = oldMap.entrySet();
      var news = newMap.entrySet();

      checkNoUpdatesOrThrow(oldMap, newMap, olds, news);

      var added = new HashSet<Map.Entry<String, String>>(news);
      // Get added keys. From new set remove
      // - keys already present in old set
      added.removeIf(arg0 -> {
         return oldMap.containsKey(arg0.getKey());
      });

      var removed = new HashSet<Map.Entry<String, String>>(olds);
      // Get removed keys. From old set remove
      // - keys still present in new set
      removed.removeIf((arg0 -> {
         return newMap.containsKey(arg0.getKey());
      }));

      processConfigMapAndSend(removed, (name, rule) -> {
         log.debug("calling DynamicRuleManagement.removeRule({},{}", name, rule.toString());
         drm.removeRule(name);
      });
      processConfigMapAndSend(added, (name, rule) -> {
         log.debug("calling DynamicRuleManagement.addRule({},{}", name, rule.toString());
         drm.addRule(name, new EagerCacheRuleSpecAdapter(rule));
      });
   }

   private void checkNoUpdatesOrThrow(Map<String, String> oldMap, Map<String, String> newMap,
                                      Set<Map.Entry<String, String>> olds, Set<Map.Entry<String, String>> news) {
      var changedNewValues = new HashSet<Map.Entry<String, String>>(news);
      // Get the changed keys. From the new set remove
      // - keys which aren't in the old set (added)
      // - keys which are in the new set with same value (unchanged)
      changedNewValues.removeIf(
            arg0 -> !oldMap.containsKey(arg0.getKey()) || arg0.getValue().equals(oldMap.get(arg0.getKey())));
      if (changedNewValues.size() > 0) {
         // Update rule is unsupported and this should never happens. Assuption here is
         // that the configMap
         // is corrupted, so we raise an exception and no events are sent to the rule
         // manager. Exception contains rule with old and new values
         var changedOldValues = new HashSet<Map.Entry<String, String>>(olds);
         changedOldValues.removeIf(
               arg0 -> !newMap.containsKey(arg0.getKey()) || arg0.getValue().equals(newMap.get(arg0.getKey())));
         throw new UnsupportedOperationException(
               "Rules cannot be updated: new values " + changedNewValues.toString() + ", old values "
                     + changedOldValues.toString());
      }
   }

   @Override
   public void onDelete(ConfigMap obj, boolean deletedFinalStateUnknown) {
      processConfigMapAndSend(obj.getData().entrySet(), (name, rule) -> {
         log.debug("calling DynamicRuleManagement.removeRule({},{}", name, rule.toString());
         drm.removeRule(name);
      });
   }

   private interface SendEventFunc {
      void sendEvent(String name, EagerCacheRuleSpec rule);
   }

   // Used only by CacheRuleInformer
   public static class EagerCacheRuleSpecAdapter implements Rule {
      private final EagerCacheRuleSpec eagerRule;

      public EagerCacheRuleSpecAdapter(EagerCacheRuleSpec eagerRule) {
         this.eagerRule = eagerRule;
      }

      private record ConnectorImplementation(String[] schemaTable) implements Connector {

         @Override
         public String schema() {
            return schemaTable[0];
         }

         @Override
         public String table() {
            return schemaTable[1];
         }
      }

      @Override
      public Connector connector() {
         String[] schemaTable = eagerRule.getTableName().split("\\.");
         return switch (schemaTable.length) {
            case 1 -> new Connector() {
               @Override public String schema() {
                  return "";
               }

               @Override public String table() {
                  return schemaTable[0];
               }
            };
            case 2 -> new ConnectorImplementation(schemaTable);
            default -> throw new IllegalArgumentException("Unsupported schema format (must be table or schema.table)");
         };
      }

      @Override
      public KeyFormat keyType() {
         return eagerRule.getKey().getFormat();
      }

      @Override
      public String plainSeparator() {
         return eagerRule.getKey().getKeySeparator();
      }

      @Override
      public List<String> keyColumns() {
         return eagerRule.getKey().getKeyColumnsList();
      }

      @Override
      public Optional<List<String>> valueColumns() {
         var list = eagerRule.getValue().getValueColumnsList();
         return Optional.ofNullable(list.size() > 0 ? list : null);
      }
   }
}
