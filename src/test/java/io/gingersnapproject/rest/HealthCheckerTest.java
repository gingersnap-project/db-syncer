package io.gingersnapproject.rest;

import static io.gingersnapproject.testcontainers.BaseGingersnapResourceLifecycleManager.REMOVE_DEFAULT_RULE;
import static io.restassured.RestAssured.given;

import io.gingersnapproject.testcontainers.annotation.KeyValue;
import io.gingersnapproject.testcontainers.annotation.WithMySQL;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

@QuarkusTest
@WithMySQL(properties = @KeyValue(key = REMOVE_DEFAULT_RULE))
public class HealthCheckerTest {
   @Test
   public void testHealthEndpointDefined() {
      given()
            .when().get("/q/health/ready")
            .then()
            .statusCode(200);
   }
}
