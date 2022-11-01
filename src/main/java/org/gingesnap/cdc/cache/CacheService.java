package org.gingesnap.cdc.cache;

import java.net.URI;
import java.util.concurrent.CompletionStage;

import org.gingesnap.cdc.CacheBackend;
import org.gingesnap.cdc.OffsetBackend;
import org.gingesnap.cdc.SchemaBackend;
import org.gingesnap.cdc.translation.JsonTranslator;

public interface CacheService {
   boolean supportsURI(URI uri);

   CompletionStage<Boolean> reconnect(URI uri);

   CompletionStage<Void> stop(URI uri);

   CacheBackend backendForURI(URI uri, JsonTranslator<?> keyTranslator, JsonTranslator<?> valueTranslator);

   OffsetBackend offsetBackendForURI(URI uri);

   SchemaBackend schemaBackendForURI(URI uri);

   void shutdown(URI uri);
}
