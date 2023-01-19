package io.gingersnapproject.k8s;

import java.util.ArrayList;
import java.util.List;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;

import io.gingersnapproject.k8s.informer.KubernetesInformer;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.quarkus.arc.All;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class CacheInformerManager {

   private static final Logger log = LoggerFactory.getLogger(CacheInformerManager.class);

   @Inject
   Instance<KubernetesClient> client;

   @All
   @Inject
   List<KubernetesInformer<?>> informers;

   private final List<SharedIndexInformer<?>> runningInformers = new ArrayList<>();

   void startWatching(@Observes StartupEvent ignore) {
      if (client.isUnsatisfied()) {
         log.info("Kubernetes client not found, not registering informers");
         return;
      }

      KubernetesClient kc = client.get();
      for (var informer : informers) {
         var registered = informer.register(kc);
         runningInformers.add(registered);
         registered.start();
      }
   }

   public void stop(@Observes ShutdownEvent ignore) {
      log.info("De-registering informers");

      var iterator = runningInformers.iterator();
      while (iterator.hasNext()) {
         iterator.next().stop();
         iterator.remove();
      }
   }

}
