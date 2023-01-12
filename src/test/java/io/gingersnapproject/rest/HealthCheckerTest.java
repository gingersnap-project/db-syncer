package io.gingersnapproject.rest;

import static io.restassured.RestAssured.given;

import io.gingersnapproject.testcontainers.annotation.WithDatabase;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

@QuarkusTest
@WithDatabase(rules = "rule1")
public class HealthCheckerTest {
   @Test
   public void testHealthEndpointDefined() {
      given()
            .when().get("/q/health/ready")
            .then()
            .statusCode(200);
   }
}
