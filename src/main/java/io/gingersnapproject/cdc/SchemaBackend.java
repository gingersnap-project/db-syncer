package io.gingersnapproject.cdc;

import java.util.function.Consumer;

import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.SchemaHistoryException;

public interface SchemaBackend {
   void storeRecord(HistoryRecord record) throws SchemaHistoryException;

   void recoverRecords(Consumer<HistoryRecord> records) throws SchemaHistoryException;

   boolean schemaExists();
}
