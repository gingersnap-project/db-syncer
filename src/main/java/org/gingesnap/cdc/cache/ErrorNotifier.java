package org.gingesnap.cdc.cache;

import org.gingesnap.cdc.EngineWrapper;
import org.gingesnap.cdc.ManagedEngine;

import io.quarkus.arc.Arc;

public class ErrorNotifier {
   public static void notifyError(String topic) {
      Arc.container().instance(ManagedEngine.class).get().engineError(topic);
   }
}
