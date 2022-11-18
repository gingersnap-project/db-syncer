package io.gingersnap_project.cdc.cache;

import io.gingersnap_project.cdc.ManagedEngine;

import io.quarkus.arc.Arc;

public class ErrorNotifier {
   public static void notifyError(String topic) {
      Arc.container().instance(ManagedEngine.class).get().engineError(topic);
   }
}
