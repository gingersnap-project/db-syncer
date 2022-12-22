package io.gingersnapproject.testcontainers.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import io.gingersnapproject.testcontainers.MySQLResources;

import io.quarkus.test.common.QuarkusTestResource;

@QuarkusTestResource(value = MySQLResources.class, restrictToAnnotatedClass = true, parallel = true)
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface WithMySQL {

   KeyValue[] properties() default {};
}
