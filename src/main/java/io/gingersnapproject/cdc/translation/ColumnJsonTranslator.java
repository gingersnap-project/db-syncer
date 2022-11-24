package io.gingersnapproject.cdc.translation;

import java.util.List;

import org.infinispan.commons.dataconversion.internal.Json;

public class ColumnJsonTranslator implements JsonTranslator<Json> {
   private final List<String> columns;

   public ColumnJsonTranslator(List<String> columns) {
      if (columns.size() < 1) {
         throw new IllegalArgumentException("Number of arguments needs to be one or more");
      }
      this.columns = columns;
   }

   @Override
   public Json apply(Json json) {
      Json jsonView = Json.object();
      return applyExisting(json, jsonView);
   }

   public Json applyExisting(Json newElements, Json destination) {
      for (String column : columns) {
         destination.set(column, newElements.at(column));
      }
      return destination;
   }
}
