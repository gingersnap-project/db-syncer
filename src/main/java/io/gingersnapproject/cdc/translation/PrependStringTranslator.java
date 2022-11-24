package io.gingersnapproject.cdc.translation;

import org.infinispan.commons.dataconversion.internal.Json;

public class PrependStringTranslator implements JsonTranslator<StringBuilder> {
   private final ColumnStringTranslator translator;
   private final String prefix;

   public PrependStringTranslator(ColumnStringTranslator translator, String prefix) {
      this.translator = translator;
      this.prefix = prefix;
   }

   @Override
   public StringBuilder apply(Json json) {
      StringBuilder newBuilder = new StringBuilder(prefix);
      newBuilder.append(translator.getSeparator());

      StringBuilder translated = translator.apply(json);

      return newBuilder.append(translated);
   }
}
