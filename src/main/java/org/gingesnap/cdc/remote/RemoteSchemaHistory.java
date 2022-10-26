package org.gingesnap.cdc.remote;

import java.net.URI;
import java.util.function.Consumer;

import org.gingesnap.cdc.SchemaBackend;
import org.gingesnap.cdc.cache.CacheService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.relational.history.AbstractSchemaHistory;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.relational.history.SchemaHistoryException;
import io.debezium.relational.history.SchemaHistoryListener;
import io.quarkus.arc.Arc;
import io.quarkus.arc.InstanceHandle;

public class RemoteSchemaHistory extends AbstractSchemaHistory {
   private static final Logger log = LoggerFactory.getLogger(RemoteSchemaHistory.class);
   public static final String URI_CACHE = CONFIGURATION_FIELD_PREFIX_STRING + "remote.uri";

   private SchemaBackend schemaBackend;

   @Override
   protected void storeRecord(HistoryRecord record) throws SchemaHistoryException {
      log.info("Storing schema history record {}", record);
      schemaBackend.storeRecord(record);
   }

   @Override
   protected void recoverRecords(Consumer<HistoryRecord> records) {
      log.info("Recovering schema history records");
      schemaBackend.recoverRecords(records);
   }

   @Override
   public void configure(Configuration config, HistoryRecordComparator comparator, SchemaHistoryListener listener, boolean useCatalogBeforeSchema) {
      super.configure(config, comparator, listener, useCatalogBeforeSchema);
      String stringURI = config.getString(URI_CACHE);
      URI uri = URI.create(stringURI);
      for (InstanceHandle<CacheService> instanceHandle : Arc.container().listAll(CacheService.class)) {
         CacheService cacheService = instanceHandle.get();
         schemaBackend = cacheService.schemaBackendForURI(uri);
         if (schemaBackend != null) {
            break;
         }
      }
      if (schemaBackend == null) {
         throw new IllegalStateException("No schema cache storage for uri: " + uri);
      }
   }

   @Override
   public boolean exists() {
      return schemaBackend != null && schemaBackend.schemaExists();
   }

   @Override
   public boolean storageExists() {
      return schemaBackend != null;
   }
}
