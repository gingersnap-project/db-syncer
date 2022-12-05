package io.gingersnapproject.k8s.configuration;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.jboss.logging.Logger;

import io.gingersnapproject.cdc.configuration.Database;
import io.gingersnapproject.cdc.connector.DatabaseProvider;
import io.quarkus.kubernetes.service.binding.runtime.ServiceBinding;
import io.quarkus.kubernetes.service.binding.runtime.ServiceBindingConfigSource;
import io.quarkus.kubernetes.service.binding.runtime.ServiceBindingConverter;

public class DatabaseServiceBindingConverter implements ServiceBindingConverter {

   private static final Logger log = Logger.getLogger(DatabaseServiceBindingConverter.class);

   static Set<String> supportedTypes = Arrays.stream(DatabaseProvider.values())
         .map(Objects::toString)
         .map(String::toLowerCase)
         .collect(Collectors.toSet());

   @Override
   public Optional<ServiceBindingConfigSource> convert(List<ServiceBinding> allBindings) {
      // Process multiple bindings in sorted order to ensure behaviour is deterministic
      var matchingBindings = allBindings.stream()
            .filter(sb -> supportedTypes.contains(sb.getType()))
            .sorted(Comparator.comparing(ServiceBinding::getType))
            .toList();

      if (matchingBindings.isEmpty()) {
         log.debugf("No Database ServiceBounding found: %s", allBindings.stream().map(ServiceBinding::getType).collect(Collectors.joining(",")));
         return Optional.empty();
      }

      ServiceBinding binding = matchingBindings.get(0);
      if (matchingBindings.size() > 1) {
         String types = matchingBindings.stream().map(ServiceBinding::getType).collect(Collectors.joining(","));
         log.warnf("Multiple Database ServiceBinding types found '%s', but only '%s' with type '%s' will be used", types, binding.getName(), binding.getType());
      }

      String type = binding.getType();
      String name = String.format("gingersnap-database-%s-k8s-service-binding-source", type);
      Map<String, String> properties = binding.getProperties()
            .entrySet()
            .stream()
            .collect(Collectors.toMap(e -> Database.property(e.getKey()), Map.Entry::getValue));
      properties.put(Database.property("type"), type);
      return Optional.of(new ServiceBindingConfigSource(name, properties));
   }
}
