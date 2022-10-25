package org.gingesnap.cdc;

import java.util.concurrent.CompletionStage;

import org.infinispan.commons.dataconversion.internal.Json;

public interface CacheBackend {
   CompletionStage<Void> remove(String key);

   CompletionStage<Void> put(String key, Json json);
}
