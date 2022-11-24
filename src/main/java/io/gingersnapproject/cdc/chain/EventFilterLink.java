package io.gingersnapproject.cdc.chain;

import io.gingersnapproject.cdc.configuration.Rule;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Link responsible for filtering {@link Event}.
 * <p>The link aborts the execution if the event is not acceptable.</p>
 * <p>
 *    <h3>It should be the first link in the chain.</h3>
 * </p>
 *
 * @see EventProcessingChain
 * @author Jose Bolina
 */
public class EventFilterLink extends EventProcessingChain {

   private static final Logger log = LoggerFactory.getLogger(EventFilterLink.class);

   private final Rule.SingleRule rule;

   public EventFilterLink(Rule.SingleRule rule) {
      this.rule = rule;
   }

   @Override
   public boolean process(Event event, EventContext ctx) {
      if (!acceptEvent(event)) {
         log.warn("Discarded event {}", event);
         return false;
      }

      String table = event.value().at("source").at("table").asString();
      return rule.connector().table().equals(table) && processNext(event, ctx);
   }

   /**
    * Verify if the {@link Event} is acceptable. The event must:
    * <lu>
    *    <li>Not be a DDL change;</li>
    *    <li>Generated from the registered table.</li>
    * </lu>
    *
    * @param event: Event to check.
    * @return true if accepted, and false otherwise.
    */
   private boolean acceptEvent(Event event) {
      if (isDdl(event)) return false;

      return event.value().has("source") && event.value().at("source").has("table");
   }

   private boolean isDdl(Event event) {
      return event.value().has("ddl") && event.value().at("ddl") != null;
   }
}
