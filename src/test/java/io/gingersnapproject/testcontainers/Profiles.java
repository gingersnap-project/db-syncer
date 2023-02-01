package io.gingersnapproject.testcontainers;

import static org.junit.platform.commons.util.AnnotationUtils.findAnnotation;

import java.util.Set;

import io.gingersnapproject.testcontainers.annotation.WithDatabase;
import io.gingersnapproject.testcontainers.database.MySQL;
import io.gingersnapproject.testcontainers.database.Oracle;
import io.gingersnapproject.testcontainers.database.Postgres;
import io.gingersnapproject.testcontainers.hotrod.CacheManagerContainer;
import io.gingersnapproject.testcontainers.hotrod.HotRodContainer;
import io.gingersnapproject.testcontainers.hotrod.InfinispanContainer;

import io.quarkus.runtime.LaunchMode;
import io.quarkus.runtime.configuration.ProfileManager;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

public class Profiles {
   private static final Set<String> ACTIVE_PROFILES;

   static {
      ProfileManager.setLaunchMode(LaunchMode.TEST);
      var quarkusProfile = ProfileManager.getActiveProfile().replace(" ", "");
      ACTIVE_PROFILES = Set.of(quarkusProfile.split(","));
   }

   public static Class<? extends DatabaseProvider> databaseProviderClass() {
      if (isProfileActive("postgres")) return Postgres.class;
      if (isProfileActive("oracle")) return Oracle.class;
      return MySQL.class;
   }

   public static Class<? extends HotRodContainer<?>> hotRodContainerClass() {
      if (isProfileActive("infinispan")) return InfinispanContainer.class;
      return CacheManagerContainer.class;
   }

   public static class ProfileCondition implements ExecutionCondition {

      @Override
      public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
         if (!ProfileManager.getLaunchMode().isDevOrTest())
            return ConditionEvaluationResult.disabled(String.format("No test running for '%s' mode!", ProfileManager.getLaunchMode()));

         final var optional = findAnnotation(context.getElement(), WithDatabase.class);
         if (optional.isPresent()) {
            if (Boolean.parseBoolean(System.getProperty("functionalTestOnly", "false")))
               return ConditionEvaluationResult.disabled("Running only functional tests");

            var annotation = optional.get();
            Class<? extends DatabaseProvider> dp = annotation.value();
            if (dp.isInterface()) return ConditionEvaluationResult.enabled("Database defined by profile");

            var clazz = Profiles.databaseProviderClass();
            return dp.equals(clazz)
                  ? ConditionEvaluationResult.enabled(String.format("Filtered to use database '%s' as profile", dp))
                  : ConditionEvaluationResult.disabled(String.format("Test filtered to use another database type '%s' instead of '%s'", dp, clazz));
         }

         return isProfileActive(LaunchMode.TEST.getDefaultProfile())
               ? ConditionEvaluationResult.enabled("Test without database can run")
               : ConditionEvaluationResult.disabled(String.format("Test without database and profile '%s' not running", ACTIVE_PROFILES));
      }
   }

   public static boolean isProfileActive(String profile) {
      return ACTIVE_PROFILES.contains(profile);
   }
}
