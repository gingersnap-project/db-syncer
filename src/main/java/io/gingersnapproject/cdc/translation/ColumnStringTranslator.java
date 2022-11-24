package io.gingersnapproject.cdc.translation;

import java.util.List;

import org.infinispan.commons.dataconversion.internal.Json;

public class ColumnStringTranslator implements JsonTranslator<StringBuilder> {
   private final List<String> columns;
   private final String separator;

   public ColumnStringTranslator(List<String> columns, String separator) {
      if (columns.size() < 1) {
         throw new IllegalArgumentException("Number of arguments needs to be one or more");
      }
      this.columns = columns;
      this.separator = separator;
   }

   public List<String> getColumns() {
      return columns;
   }

   public String getSeparator() {
      return separator;
   }

   @Override
   public StringBuilder apply(Json json) {
      StringBuilder builder = new StringBuilder();
      for (int i = 0; i < columns.size(); ++i) {
         builder.append(json.at(columns.get(i)).asString());
         if (i != columns.size() - 1) {
            builder.append(separator);
         }
      }
      return builder;
   }
}
