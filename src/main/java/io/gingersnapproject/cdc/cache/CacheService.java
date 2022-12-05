package io.gingersnapproject.cdc.cache;

import java.util.concurrent.CompletionStage;

import io.gingersnapproject.cdc.CacheBackend;
import io.gingersnapproject.cdc.OffsetBackend;
import io.gingersnapproject.cdc.SchemaBackend;
import io.gingersnapproject.cdc.configuration.Rule;

public interface CacheService {
   CompletionStage<Boolean> reconnect();

   CompletionStage<Void> stop();

   CacheBackend backendForRule(String name, Rule rule);

   OffsetBackend offsetBackend();

   SchemaBackend schemaBackend();
}
