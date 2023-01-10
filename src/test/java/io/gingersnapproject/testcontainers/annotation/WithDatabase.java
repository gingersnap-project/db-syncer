package io.gingersnapproject.testcontainers.annotation;

import java.lang.annotation.Annotation;
import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import io.gingersnapproject.testcontainers.BaseGingersnapResourceLifecycleManager;
import io.gingersnapproject.testcontainers.DatabaseProvider;

import io.quarkus.test.common.QuarkusTestResource;

@Repeatable(value = WithDatabase.WithDatabases.class)
@QuarkusTestResource(value = BaseGingersnapResourceLifecycleManager.class, restrictToAnnotatedClass = true, parallel = true)
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface WithDatabase {

   KeyValue[] properties() default {};

   Class<? extends DatabaseProvider> value();

   String[] rules() default {};

   @Target(ElementType.TYPE)
   @Retention(RetentionPolicy.RUNTIME)
   @QuarkusTestResource(value = BaseGingersnapResourceLifecycleManager.RepeatableGingersnapResource.class, restrictToAnnotatedClass = true, parallel = true)
   @interface WithDatabases {
      WithDatabase[] value();
   }
}
