package io.gingersnapproject.cdc.cache;

import io.gingersnapproject.cdc.ManagedEngine;

import io.quarkus.arc.Arc;

public class ErrorNotifier {
   public static void notifyError(String topic) {
      Arc.container().instance(ManagedEngine.class).get().engineError(topic);
   }
}
