package io.gingersnapproject.testcontainers.hotrod;

import java.net.URI;

import io.restassured.response.ValidatableResponse;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

public class InfinispanContainer extends HotRodContainer<InfinispanContainer> {

   public InfinispanContainer() {
      super(DockerImageName.parse("infinispan/server:latest"));
      withExposedPorts(11222);
      waitingFor(Wait.forLogMessage(".*ISPN080001: Infinispan Server .*", 1));
      withEnv("USER", "admin");
      withEnv("PASS", "password");
   }

   @Override
   public String hotRodURI() {
      return String.format("hotrod://admin:password@%s:%d?sasl_mechanism=SCRAM-SHA-512", getHost(), getMappedPort(11222));
   }

   @Override
   public ValidatableResponse getById(String rule, String id) {
      return getById(URI.create(String.format("http://%s:%d/rest/v2/caches/%s/%s", getHost(), getMappedPort(11222), rule, id)));
   }

   @Override
   public String username() {
      return "admin";
   }

   @Override
   public String password() {
      return "password";
   }
}
