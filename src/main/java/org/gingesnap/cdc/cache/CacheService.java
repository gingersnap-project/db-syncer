package org.gingesnap.cdc.cache;

import java.net.URI;

import org.gingesnap.cdc.CacheBackend;
import org.gingesnap.cdc.OffsetBackend;
import org.gingesnap.cdc.SchemaBackend;

public interface CacheService {
   CacheBackend backendForURI(URI uri);

   OffsetBackend offsetBackendForURI(URI uri);

   SchemaBackend schemaBackendForURI(URI uri);

   void shutdown();
}
