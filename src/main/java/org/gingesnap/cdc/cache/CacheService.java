package org.gingesnap.cdc.cache;

import java.net.URI;

import org.gingesnap.cdc.CacheBackend;

public interface CacheService {
   CacheBackend backendForURI(URI uri);

   void shutdown();
}
