package io.gingersnapproject.cdc.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.infinispan.commons.dataconversion.internal.Json;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SerializationTest {

   /**
    * Since we need to create the struct and the check the parse, we'll stick to a well defined JSON:
    *
    * <p><b>Value:</b></p>
    * <pre>{@code
    *  {
    *     "person": {
    *        "name": String,
    *        "age": int,
    *        "enabled": bool
    *     },
    *     "data": List<String>
    *  }
    * }</pre>
    */
   @Test
   public void testObjectParseCorrectly() {
      var expected = Json.object()
            .set("person", Json.object("name", "jose", "age", 25, "enabled", true))
            .set("data", Json.array("value1", "value2"));
      var struct = createStruct();

      struct.validate();

      assertEquals(expected, Serialization.convert(struct));
   }

   private Struct createStruct() {
      var personSchema = SchemaBuilder.struct()
            .field("name", Schema.STRING_SCHEMA)
            .field("age", Schema.INT32_SCHEMA)
            .field("enabled", Schema.BOOLEAN_SCHEMA)
            .build();
      var personStruct = new Struct(personSchema)
            .put("name", "jose")
            .put("age", 25)
            .put("enabled", true);

      var objectSchema = SchemaBuilder.struct()
            .field("person", personSchema)
            .field("data", SchemaBuilder.array(SchemaBuilder.STRING_SCHEMA))
            .build();
      return new Struct(objectSchema)
            .put("person", personStruct)
            .put("data", List.of("value1", "value2"));
   }
}
