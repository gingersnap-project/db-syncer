package io.gingersnapproject.k8s.configuration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.gingersnapproject.cdc.configuration.Cache;
import io.quarkus.kubernetes.service.binding.runtime.ServiceBinding;
import io.quarkus.kubernetes.service.binding.runtime.ServiceBindingConfigSource;
import io.quarkus.kubernetes.service.binding.runtime.ServiceBindingConverter;

public class CacheServiceBindingConverter implements ServiceBindingConverter {

   @Override
   public Optional<ServiceBindingConfigSource> convert(List<ServiceBinding> serviceBindings) {
      return ServiceBinding.singleMatchingByType("gingersnap", serviceBindings)
            .map(serviceBinding -> {
               String name = "gingersnap-cache-k8s-service-binding-source";
               Map<String, String> properties = new HashMap<>();
               Map<String, String> bindingProperties = serviceBinding.getProperties();

               String uri = bindingProperties.get("uri");
               if (uri != null) {
                  properties.put(Cache.property("uri"), uri);
                  return new ServiceBindingConfigSource(name, properties);
               }

               String host = bindingProperties.get("host");
               String port = bindingProperties.get("port");

               String username = bindingProperties.get("username");
               String password = bindingProperties.get("password");
               if (username != null && password != null) {
                  uri = String.format("hotrod://%s:%s@%s:%s?sasl_mechanism=SCRAM-SHA-512", username, password, host, port);
               } else {
                  uri = String.format("hotrod://%s:%s", host, port);
               }
               properties.put(Cache.property("uri"), uri);
               return new ServiceBindingConfigSource(name, properties);
            });
   }
}
