package io.gingersnapproject.cdc.cache.hotrod;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import org.apache.kafka.connect.util.Callback;
import io.gingersnapproject.cdc.OffsetBackend;
import org.infinispan.client.hotrod.RemoteCache;

public class HotRodOffsetBackend implements OffsetBackend {
   private final RemoteCache<byte[], byte[]> remoteCache;
   public HotRodOffsetBackend(RemoteCache<byte[],byte[]> remoteCache) {
      this.remoteCache = remoteCache;
   }

   // This method assumes the ByteBuffer is backed by an array
   static byte[] byteBufferToByteArray(ByteBuffer bb) {
      byte[] byteArray = bb.array();
      if (bb.position() == 0 && bb.limit() == byteArray.length) {
         return byteArray;
      }
      return Arrays.copyOfRange(byteArray, bb.position(), bb.limit());
   }

   @Override
   public CompletionStage<Map<ByteBuffer, ByteBuffer>> get(Collection<ByteBuffer> collection) {
      return remoteCache.getAllAsync(collection.stream().map(HotRodOffsetBackend::byteBufferToByteArray).collect(Collectors.toSet()))
            .thenApply(map -> map.entrySet().stream().collect(Collectors.toMap(e -> ByteBuffer.wrap(e.getKey()), e -> ByteBuffer.wrap(e.getValue()))));
   }

   @Override
   public CompletionStage<Void> set(Map<ByteBuffer, ByteBuffer> map, Callback<Void> callback) {
      CompletableFuture<Void> future = remoteCache.putAllAsync(map.entrySet().stream().collect(Collectors.toMap(e -> HotRodOffsetBackend.byteBufferToByteArray(e.getKey()),
            e -> HotRodOffsetBackend.byteBufferToByteArray(e.getValue()))));
      return future.whenComplete((v, t) -> callback.onCompletion(t, v));
   }
}
