package io.gingersnapproject.k8s.informer;

import java.util.Set;

import javax.enterprise.context.ApplicationScoped;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class PodInformer implements KubernetesInformer<Pod> {
   private static final Logger log = LoggerFactory.getLogger(PodInformer.class);
   private static final Set<String> VALID_HR_IMAGES = Set.of(
         "quay.io/gingersnap/cache-manager",
         "infinispan/server"
   );

   @Override
   public SharedIndexInformer<Pod> register(KubernetesClient kc) {
      return kc.pods().inform(this, 30 * 1000);
   }

   @Override
   public void onAdd(Pod obj) {
      if (isHotRodPod(obj)) log.info("Adding HR pod {}", obj);
   }

   @Override
   public void onUpdate(Pod oldObj, Pod newObj) { }

   @Override
   public void onDelete(Pod obj, boolean deletedFinalStateUnknown) {
      if (isHotRodPod(obj)) log.info("Removing HR pod {}", obj);
   }

   private static boolean isHotRodPod(Pod obj) {
      for (var container : obj.getSpec().getContainers()) {
         for (var validImage : VALID_HR_IMAGES) {
            if (container.getImage().startsWith(validImage))
               return true;
         }
      }

      return false;
   }
}
