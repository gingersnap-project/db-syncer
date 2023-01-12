package io.gingersnapproject.testcontainers.hotrod;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

public abstract class HotRodContainer<T extends HotRodContainer<T>> extends GenericContainer<T> {

   public HotRodContainer(DockerImageName name) {
      super(name);
   }

   public String hotRodURI() {
      return String.format("hotrod://%s:%d", getHost(), getMappedPort(11222));
   }
}
