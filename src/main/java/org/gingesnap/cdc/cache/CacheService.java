package org.gingesnap.cdc.cache;

import java.net.URI;

import org.gingesnap.cdc.CacheBackend;
import org.gingesnap.cdc.OffsetBackend;

public interface CacheService {
   CacheBackend backendForURI(URI uri);

   OffsetBackend offsetBackendForURI(URI uri);

   void shutdown();
}
