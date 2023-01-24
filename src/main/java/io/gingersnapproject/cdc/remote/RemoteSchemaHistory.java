package io.gingersnapproject.cdc.remote;

import java.net.URI;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.relational.history.AbstractSchemaHistory;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.relational.history.SchemaHistoryException;
import io.debezium.relational.history.SchemaHistoryListener;
import io.gingersnapproject.cdc.SchemaBackend;
import io.gingersnapproject.cdc.cache.CacheService;
import io.gingersnapproject.cdc.event.NotificationManager;
import io.gingersnapproject.util.ArcUtil;

public class RemoteSchemaHistory extends AbstractSchemaHistory {
   private static final Logger log = LoggerFactory.getLogger(RemoteSchemaHistory.class);
   public static final String URI_CACHE = CONFIGURATION_FIELD_PREFIX_STRING + "remote.uri";
   public static final String TOPIC_NAME = CONFIGURATION_FIELD_PREFIX_STRING + "remote.topic";

   private NotificationManager eventing;
   private SchemaBackend schemaBackend;
   private String topicName;

   @Override
   protected void storeRecord(HistoryRecord record) throws SchemaHistoryException {
      log.info("Storing schema history record {}", record);
      try {
         schemaBackend.storeRecord(record);
      } catch (Throwable t) {
         eventing.connectorFailed(topicName, t);
         throw t;
      }
   }

   @Override
   protected void recoverRecords(Consumer<HistoryRecord> records) throws SchemaHistoryException {
      log.info("Recovering schema history records");
      try {
         schemaBackend.recoverRecords(records);
      } catch (Throwable t) {
         eventing.connectorFailed(topicName, t);
         throw t;
      }
   }

   @Override
   public void configure(Configuration config, HistoryRecordComparator comparator, SchemaHistoryListener listener, boolean useCatalogBeforeSchema) {
      super.configure(config, comparator, listener, useCatalogBeforeSchema);
      topicName = config.getString(TOPIC_NAME);
      String uri = config.getString(URI_CACHE);
      CacheService cacheService = ArcUtil.instance(CacheService.class);
      schemaBackend = cacheService.schemaBackend(URI.create(uri));
      eventing = ArcUtil.instance(NotificationManager.class);
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
