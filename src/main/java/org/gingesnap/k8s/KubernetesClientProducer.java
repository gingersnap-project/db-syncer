package org.gingesnap.k8s;

import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import javax.inject.Singleton;

import org.gingesnap.k8s.configuration.KubernetesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.quarkus.arc.lookup.LookupUnlessProperty;

@Singleton
public class KubernetesClientProducer {

   private static final Logger log = LoggerFactory.getLogger(KubernetesClientProducer.class);

   @Inject KubernetesConfiguration configuration;

   @Produces
   @LookupUnlessProperty(name = "gingersnap.k8s.rule-config-map", stringValue = "")
   public KubernetesClient kubernetesClient() {
      log.info("Creating Kubernetes client");

      ConfigBuilder cb = new ConfigBuilder();
      return new KubernetesClientBuilder()
            .withConfig(cb.withNamespace(configuration.namespace()).build())
            .build();
   }
}
