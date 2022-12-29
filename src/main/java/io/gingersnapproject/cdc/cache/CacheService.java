package io.gingersnapproject.cdc.cache;

import java.util.concurrent.CompletionStage;

import io.gingersnapproject.cdc.CacheBackend;
import io.gingersnapproject.cdc.OffsetBackend;
import io.gingersnapproject.cdc.SchemaBackend;
import io.gingersnapproject.cdc.configuration.Rule;

public interface CacheService {
   CompletionStage<Boolean> reconnect(String name, Rule rule);

   void stop(String name);

   CacheBackend backendForRule(String name, Rule rule);

   OffsetBackend offsetBackend();

   SchemaBackend schemaBackend();
}
