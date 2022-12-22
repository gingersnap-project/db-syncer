package io.gingersnapproject.rest;

import static io.restassured.RestAssured.given;

import io.gingersnapproject.testcontainers.database.MySQL;
import io.gingersnapproject.testcontainers.annotation.WithDatabase;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

@QuarkusTest
@WithDatabase(value = MySQL.class)
public class HealthCheckerTest {
   @Test
   public void testHealthEndpointDefined() {
      given()
            .when().get("/q/health/ready")
            .then()
            .statusCode(200);
   }
}
