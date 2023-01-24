package io.gingersnapproject.cdc;

import java.util.concurrent.CompletionStage;

import org.infinispan.commons.dataconversion.internal.Json;

public interface CacheBackend {
   CompletionStage<Void> remove(Json json);

   CompletionStage<Void> put(Json json);

   void stop();

   void start();

   boolean reconnect();

   boolean isRunning();
}
