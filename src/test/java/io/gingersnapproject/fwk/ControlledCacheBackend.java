package io.gingersnapproject.fwk;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.function.UnaryOperator;

import io.gingersnapproject.cdc.CacheBackend;

import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.util.concurrent.CompletableFutures;

public class ControlledCacheBackend implements CacheBackend {
   private final Map<Json, Json> data = new HashMap<>();
   private final UnaryOperator<Json> keyExtraction;
   private CompletionStage<Void> nextAnswer = CompletableFutures.completedNull();

   public ControlledCacheBackend(UnaryOperator<Json> keyExtraction) {
      this.keyExtraction = keyExtraction;
   }

   @Override
   public CompletionStage<Void> remove(Json json) {
      Objects.requireNonNull(keyExtraction.apply(json), "Removed object must have a key");
      data.remove(keyExtraction.apply(json));
      return nextAnswer;
   }

   @Override
   public CompletionStage<Void> put(Json json) {
      Objects.requireNonNull(keyExtraction.apply(json), "Added object must have a key");
      data.put(keyExtraction.apply(json), json);
      return nextAnswer;
   }

   @Override
   public void stop() { }

   @Override
   public void start() { }

   @Override
   public boolean reconnect() {
      return true;
   }

   @Override
   public boolean isRunning() {
      return false;
   }

   public Collection<Json> data() {
      return data.values();
   }

   public void setNextAnswer(CompletionStage<Void> answer) {
      nextAnswer = answer;
   }

   public void clear() {
      data.clear();
      nextAnswer = CompletableFutures.completedNull();
   }
}
