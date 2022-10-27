package org.gingesnap.cdc.remote;

import java.net.URI;
import java.util.function.Consumer;

import org.gingesnap.cdc.SchemaBackend;
import org.gingesnap.cdc.cache.CacheService;
import org.gingesnap.cdc.cache.ErrorNotifier;
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
   public static final String TOPIC_NAME = CONFIGURATION_FIELD_PREFIX_STRING + "remote.topic";

   private SchemaBackend schemaBackend;
   private String topicName;

   @Override
   protected void storeRecord(HistoryRecord record) throws SchemaHistoryException {
      log.info("Storing schema history record {}", record);
      try {
         schemaBackend.storeRecord(record);
      } catch (Throwable t) {
         ErrorNotifier.notifyError(topicName);
         throw t;
      }
   }

   @Override
   protected void recoverRecords(Consumer<HistoryRecord> records) throws SchemaHistoryException {
      log.info("Recovering schema history records");
      try {
         schemaBackend.recoverRecords(records);
      } catch (Throwable t) {
         ErrorNotifier.notifyError(topicName);
         throw t;
      }
   }

   @Override
   public void configure(Configuration config, HistoryRecordComparator comparator, SchemaHistoryListener listener, boolean useCatalogBeforeSchema) {
      super.configure(config, comparator, listener, useCatalogBeforeSchema);
      String topicName = config.getString(TOPIC_NAME);
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
