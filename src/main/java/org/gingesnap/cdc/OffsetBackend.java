package org.gingesnap.cdc;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Future;

import org.apache.kafka.connect.util.Callback;

public interface OffsetBackend {
   // TODO: make these completion stage?
   Future<Map<ByteBuffer, ByteBuffer>> get(Collection<ByteBuffer> collection);

   Future<Void> set(Map<ByteBuffer, ByteBuffer> map, Callback<Void> callback);
}
