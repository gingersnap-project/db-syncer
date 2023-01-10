package io.gingersnapproject.testcontainers;

import static org.junit.platform.commons.util.AnnotationUtils.findAnnotation;

import io.gingersnapproject.testcontainers.annotation.WithDatabase;
import io.gingersnapproject.testcontainers.database.MySQL;
import io.gingersnapproject.testcontainers.database.Postgres;

import io.quarkus.runtime.configuration.ProfileManager;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

public class Profiles {

   public static Class<? extends DatabaseProvider> provider() {
      String profile = ProfileManager.getActiveProfile();
      return switch (profile.toLowerCase()) {
         case "postgres" -> Postgres.class;
         default -> MySQL.class;
      };
   }

   public static class ProfileCondition implements ExecutionCondition {

      @Override
      public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
         final var optional = findAnnotation(context.getElement(), WithDatabase.class);
         if (optional.isPresent()) {
            var annotation = optional.get();
            Class<? extends DatabaseProvider> dp = annotation.value();
            if (dp.isInterface()) return ConditionEvaluationResult.enabled("Database defined by profile");

            return dp.getName().equals(Profiles.provider().getName())
                  ? ConditionEvaluationResult.enabled("Filtered to use same database as profile")
                  : ConditionEvaluationResult.disabled("Test filtered to use another database type");
         }
         return ConditionEvaluationResult.enabled("Test without database can run");
      }
   }
}
