package io.gingersnapproject.testcontainers.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import io.gingersnapproject.testcontainers.MySQLResources;
import io.gingersnapproject.testcontainers.PostgresResources;

import io.quarkus.test.common.QuarkusTestResource;

@QuarkusTestResource(MySQLResources.class)
@QuarkusTestResource(PostgresResources.class)
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface AllProvidersTest {
}
