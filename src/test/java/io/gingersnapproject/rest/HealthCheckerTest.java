package io.gingersnapproject.rest;

import static io.restassured.RestAssured.given;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

@QuarkusTest
public class HealthCheckerTest {

   @Test
   public void testHealthEndpointDefined() {
      given()
            .when().get("/q/health/ready")
            .then()
            .statusCode(503); // Unavailable as we dont have any service running.
   }
}
