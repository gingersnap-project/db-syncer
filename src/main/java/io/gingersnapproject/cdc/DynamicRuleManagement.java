package io.gingersnapproject.cdc;

import io.gingersnapproject.cdc.configuration.Rule;

/**
 * Responsible for handling rules dynamically.
 *
 * <p>Implementations of this class can add and remove rules. A dynamic rule is kept in memory
 * and is not persistent through restarts. In contrast, rules statically added in the application's
 * properties are restored on start. A client is responsible for adding the rules after a restart.</p>
 *
 * <p>Remove can touch on both static and dynamic rules. A static rule removed during execution will
 * be added again on restart. Before the removal, the rule is shut down, flushing all the pending work.</p>
 *
 * @author Jose Bolina
 */
public interface DynamicRuleManagement {

   void addRule(String name, Rule rule);

   void removeRule(String name);
}
