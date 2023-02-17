package io.gingersnapproject.testcontainers.hotrod;

import static io.restassured.RestAssured.given;

import java.net.URI;

import io.restassured.http.Header;
import io.restassured.response.ValidatableResponse;
import io.restassured.specification.RequestSpecification;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

public abstract class HotRodContainer<T extends HotRodContainer<T>> extends GenericContainer<T> {

   public HotRodContainer(DockerImageName name) {
      super(name);
   }

   public String hotRodURI() {
      return String.format("hotrod://%s:%d", getHost(), getMappedPort(11222));
   }

   public abstract ValidatableResponse getById(String rule, String id);

   protected ValidatableResponse getById(URI uri) {
      RequestSpecification specification = given();
      if (username() != null && password() != null) {
         specification = specification
               .auth().digest(username(), password());
      }

      return specification
            .header(new Header("Accept", "application/json"))
            .when()
            .get(uri)
            .then();
   }

   public String username() {
      return null;
   }

   public String password() {
      return null;
   }
}
