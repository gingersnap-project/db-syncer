package io.gingersnapproject.cdc.cache;

import java.net.URI;
import java.util.concurrent.CompletionStage;

import io.gingersnapproject.cdc.CacheBackend;
import io.gingersnapproject.cdc.OffsetBackend;
import io.gingersnapproject.cdc.SchemaBackend;
import io.gingersnapproject.cdc.configuration.Rule;

public interface CacheService {
   CompletionStage<Boolean> reconnect(CacheIdentifier identifier, Rule rule);

   void stop(CacheIdentifier identifier);

   CacheBackend backendForRule(CacheIdentifier identifier, Rule rule);

   OffsetBackend offsetBackend(URI managerURI);

   SchemaBackend schemaBackend(URI managerURI);
}
