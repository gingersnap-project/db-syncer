package io.gingersnapproject.cdc;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.connect.util.Callback;

public interface OffsetBackend {

   CompletionStage<Map<ByteBuffer, ByteBuffer>> get(Collection<ByteBuffer> collection);

   CompletionStage<Void> set(Map<ByteBuffer, ByteBuffer> map, Callback<Void> callback);
}
