package org.gingesnap.cdc.cache.hotrod;

import java.io.IOException;
import java.util.Collection;
import java.util.function.Consumer;

import org.gingesnap.cdc.SchemaBackend;
import org.infinispan.client.hotrod.multimap.RemoteMultimapCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.document.DocumentReader;
import io.debezium.document.DocumentWriter;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.SchemaHistoryException;

public class HotRodSchemaBackend implements SchemaBackend {
   private static final Logger log = LoggerFactory.getLogger(HotRodSchemaBackend.class);
   private final RemoteMultimapCache<String, String> multimapCache;

   public HotRodSchemaBackend(RemoteMultimapCache<String, String> multimapCache) {
      this.multimapCache = multimapCache;
   }

   @Override
   public void storeRecord(HistoryRecord record) throws SchemaHistoryException {
      try {
         multimapCache.put("schema", stringFromRecord(record)).join();
      } catch (IOException e) {
         throw new SchemaHistoryException(e);
      }
   }

   @Override
   public void recoverRecords(Consumer<HistoryRecord> records) throws SchemaHistoryException {
      Collection<String> results = multimapCache.get("schema").join();
      for (String entry : results) {
         try {
            records.accept(recordFromString(entry));
         } catch (IOException e) {
            throw new SchemaHistoryException(e);
         }
      }
   }

   private HistoryRecord recordFromString(String jsonString) throws IOException {
      return new HistoryRecord(DocumentReader.defaultReader().read(jsonString));
   }

   private String stringFromRecord(HistoryRecord record) throws IOException {
      return DocumentWriter.defaultWriter().write(record.document());
   }

   @Override
   public boolean schemaExists() {
      return multimapCache != null;
   }
}
