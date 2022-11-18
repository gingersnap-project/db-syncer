package io.gingersnap_project.cdc.translation;

import org.infinispan.commons.dataconversion.internal.Json;

public class PrependJsonTranslator implements JsonTranslator<Json> {
   private final ColumnJsonTranslator translator;
   private final String name;
   private final String prefix;

   public PrependJsonTranslator(ColumnJsonTranslator translator, String name, String prefix) {
      this.translator = translator;
      this.name = name;
      this.prefix = prefix;
   }

   @Override
   public Json apply(Json json) {
      Json newJson = Json.object();
      newJson.set(name, prefix);

      return translator.applyExisting(json, newJson);
   }
}
