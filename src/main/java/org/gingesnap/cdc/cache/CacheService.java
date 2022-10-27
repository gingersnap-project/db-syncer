package org.gingesnap.cdc.cache;

import java.net.URI;
import java.util.concurrent.CompletionStage;

import org.gingesnap.cdc.CacheBackend;
import org.gingesnap.cdc.OffsetBackend;
import org.gingesnap.cdc.SchemaBackend;

public interface CacheService {
   boolean supportsURI(URI uri);

   CompletionStage<Boolean> reconnect(URI uri);

   CompletionStage<Void> stop(URI uri);

   CacheBackend backendForURI(URI uri);

   OffsetBackend offsetBackendForURI(URI uri);

   SchemaBackend schemaBackendForURI(URI uri);

   void shutdown(URI uri);
}
