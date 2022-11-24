package io.gingersnapproject.k8s.configuration;

import java.util.Optional;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;

@ConfigMapping(prefix = "gingersnap.k8s")
public interface KubernetesConfiguration {

   @WithName("rule-config-map")
   Optional<String> configMapName();

   @WithDefault("default")
   String namespace();
}
