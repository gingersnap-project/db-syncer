package org.gingesnap.cdc;

import java.net.URI;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.dataconversion.internal.Json;

public interface CacheBackend {
   CompletionStage<Void> remove(Json json);

   CompletionStage<Void> put(Json json);

   URI uriUsed();
}
