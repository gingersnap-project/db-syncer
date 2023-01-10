package io.gingersnapproject.testcontainers.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import io.gingersnapproject.testcontainers.BaseGingersnapResourceLifecycleManager;
import io.gingersnapproject.testcontainers.DatabaseProvider;
import io.gingersnapproject.testcontainers.Profiles;

import io.quarkus.test.common.QuarkusTestResource;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(Profiles.ProfileCondition.class)
@QuarkusTestResource(value = BaseGingersnapResourceLifecycleManager.class, restrictToAnnotatedClass = true, parallel = true)
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface WithDatabase {

   KeyValue[] properties() default {};

   Class<? extends DatabaseProvider> value() default DatabaseProvider.class;

   String[] rules() default {};
}
