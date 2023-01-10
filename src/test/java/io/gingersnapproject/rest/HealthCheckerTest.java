package io.gingersnapproject.rest;

import static io.restassured.RestAssured.given;

import io.gingersnapproject.testcontainers.database.MySQL;
import io.gingersnapproject.testcontainers.annotation.WithDatabase;
import io.gingersnapproject.testcontainers.database.Postgres;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

@QuarkusTest
@WithDatabase(value = MySQL.class, rules = {"rule1", "rule2"})
@WithDatabase(value = Postgres.class, rules = "rule11")
public class HealthCheckerTest {
   @Test
   public void testHealthEndpointDefined() {
      given()
            .when().get("/q/health/ready")
            .then()
            .statusCode(200);
   }
}
