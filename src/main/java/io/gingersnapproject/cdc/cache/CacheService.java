package io.gingersnapproject.cdc.cache;

import java.io.IOException;
import java.net.URI;

import io.gingersnapproject.cdc.CacheBackend;
import io.gingersnapproject.cdc.OffsetBackend;
import io.gingersnapproject.cdc.SchemaBackend;
import io.gingersnapproject.cdc.configuration.Rule;

public interface CacheService {

   void stop(CacheIdentifier identifier);

   CacheBackend start(CacheIdentifier identifier, Rule rule) throws IOException;

   OffsetBackend offsetBackend(URI managerURI);

   SchemaBackend schemaBackend(URI managerURI);
}
