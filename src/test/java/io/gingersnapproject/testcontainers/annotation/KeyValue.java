package io.gingersnapproject.testcontainers.annotation;

public @interface KeyValue {

   String key() default "";

   String value() default "";
}
