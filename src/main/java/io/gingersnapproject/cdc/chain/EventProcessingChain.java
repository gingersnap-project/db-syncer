package io.gingersnapproject.cdc.chain;

/**
 * The base class to create a link in the event processing chain.
 * <p>Each link {@link #process(Event, EventContext)} the {@link Event} and can abort the execution or
 * invokes {@link #processNext(Event, EventContext)} to proceed to the next link in the chain. Each link
 * can change and decorate the {@link Event} as needed.</p>
 * <p>
 * <h3>The ordering of links matters!</h3>
 *
 * @see Event
 * @author Jose Bolina
 */
public abstract class EventProcessingChain {
   private EventProcessingChain next;

   public abstract boolean process(Event event, EventContext ctx);

   boolean processNext(Event event, EventContext ctx) {
      if (next == null) {
         return true;
      }
      return next.process(event, ctx);
   }

   public static EventProcessingChain chained(EventProcessingChain first, EventProcessingChain ... links) {
      EventProcessingChain head = first;
      for (EventProcessingChain link : links) {
         head.next = link;
         head = link;
      }
      return first;
   }
}
