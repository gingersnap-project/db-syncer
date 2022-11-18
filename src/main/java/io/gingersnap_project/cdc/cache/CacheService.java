package io.gingersnap_project.cdc.cache;

import java.net.URI;
import java.util.concurrent.CompletionStage;

import io.gingersnap_project.cdc.CacheBackend;
import io.gingersnap_project.cdc.OffsetBackend;
import io.gingersnap_project.cdc.SchemaBackend;
import io.gingersnap_project.cdc.translation.JsonTranslator;

public interface CacheService {
   boolean supportsURI(URI uri);

   CompletionStage<Boolean> reconnect(URI uri);

   CompletionStage<Void> stop(URI uri);

   CacheBackend backendForURI(URI uri, JsonTranslator<?> keyTranslator, JsonTranslator<?> valueTranslator);

   OffsetBackend offsetBackendForURI(URI uri);

   SchemaBackend schemaBackendForURI(URI uri);

   void shutdown(URI uri);
}
