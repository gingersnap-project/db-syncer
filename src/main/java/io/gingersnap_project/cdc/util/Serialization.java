package io.gingersnap_project.cdc.util;

import java.util.Collection;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.infinispan.commons.dataconversion.internal.Json;

/**
 * Parse a {@link Struct} to a {@link Json} object.
 * Since the list of accepted elements is not exhaustive, it might need future updates.
 */
public final class Serialization {
   private Serialization() { }

   public static Json convert(Struct struct) {
      Json json = Json.object();
      convert(struct, json);
      return json;
   }

   private static void convert(Struct struct, Json json) {
      for (Field field : struct.schema().fields()) {
         Object value = struct.get(field);
         if (value != null) {
            json.set(field.name(), convert(value));
         }
      }
   }

   private static Json convert(Collection<Object> values) {
      Json json = Json.array();
      for (Object value : values) {
         json.add(convert(value));
      }
      return json;
   }

   private static Object convert(Object value) {
      if (value instanceof Struct) return convert((Struct) value);
      if (value instanceof Collection) return convert((Collection<Object>) value);
      return value;
   }
}
