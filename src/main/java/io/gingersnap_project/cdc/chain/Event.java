package io.gingersnap_project.cdc.chain;

import org.infinispan.commons.dataconversion.internal.Json;

/**
 * Our {@link io.debezium.engine.ChangeEvent} internal representation.
 * <p>This class does not transport schema information. The key and value are direct translations from the
 * underlying event.</p>
 *
 * <p>The {@link #key} identifies the table's primary key. If the constraint does not exist, it is nullable.
 * The {@link #value} has the following properties:</p>
 * <lu>
 *    <li><i>before</i>: The value before the current change might be null in case of a create operation;</li>
 *    <li><i>after</i>: The value after the current change might be null in case of a delete operation;</li>
 *    <li><i>source</i>: Information about the database and connector;</li>
 *    <li><i>op</i>: Identify the kind of operation.</li>
 * </lu>
 *
 * @author Jose Bolina
 */
public class Event {

   private final Json key;
   private final Json value;

   public Event(Json key, Json value) {
      this.key = key;
      this.value = value;
   }

   public Json key() {
      return key;
   }

   public Json value() {
      return value;
   }

   @Override
   public String toString() {
      return "Event{" + "key=" + key + ", value=" + value + '}';
   }
}
