package io.gingersnap_project.cdc;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Future;

import org.apache.kafka.connect.util.Callback;

public interface OffsetBackend {
   // TODO: make these completion stage?
   CompletionStage<Map<ByteBuffer, ByteBuffer>> get(Collection<ByteBuffer> collection);

   CompletionStage<Void> set(Map<ByteBuffer, ByteBuffer> map, Callback<Void> callback);
}
