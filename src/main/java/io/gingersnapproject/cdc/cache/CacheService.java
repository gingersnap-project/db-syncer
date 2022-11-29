package io.gingersnapproject.cdc.cache;

import java.net.URI;
import java.util.concurrent.CompletionStage;

import io.gingersnapproject.cdc.CacheBackend;
import io.gingersnapproject.cdc.OffsetBackend;
import io.gingersnapproject.cdc.SchemaBackend;
import io.gingersnapproject.cdc.configuration.Rule;

public interface CacheService {
   boolean supportsURI(URI uri);

   CompletionStage<Boolean> reconnect(URI uri);

   CompletionStage<Void> stop(URI uri);

   CacheBackend backendForRule(String name, Rule rule);

   OffsetBackend offsetBackendForURI(URI uri);

   SchemaBackend schemaBackendForURI(URI uri);

   void shutdown(URI uri);
}
