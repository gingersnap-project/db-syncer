package io.gingersnap_project.cdc.translation;

import org.infinispan.commons.dataconversion.internal.Json;

public class IdentityTranslator implements JsonTranslator<Json> {
   private static final IdentityTranslator INSTANCE = new IdentityTranslator();

   public static IdentityTranslator getInstance() {
      return INSTANCE;
   }

   @Override
   public Json apply(Json json) {
      return json;
   }
}
