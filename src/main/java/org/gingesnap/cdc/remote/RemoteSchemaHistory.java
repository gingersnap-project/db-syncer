package org.gingesnap.cdc.remote;

import java.time.Instant;
import java.util.Map;

import io.debezium.config.Configuration;
import io.debezium.relational.Tables;
import io.debezium.relational.ddl.DdlParser;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.relational.history.SchemaHistoryException;
import io.debezium.relational.history.SchemaHistoryListener;
import io.debezium.relational.history.TableChanges;

public class RemoteSchemaHistory implements SchemaHistory {
   @Override public void configure(Configuration configuration, HistoryRecordComparator historyRecordComparator,
                                   SchemaHistoryListener schemaHistoryListener, boolean b) {

   }

   @Override public void start() {

   }

   @Override public void record(Map<String, ?> map, Map<String, ?> map1, String s, String s1)
         throws SchemaHistoryException {

   }

   @Override public void record(Map<String, ?> map, Map<String, ?> map1, String s, String s1, String s2,
                                TableChanges tableChanges, Instant instant) throws SchemaHistoryException {

   }

   @Override public void recover(Map<Map<String, ?>, Map<String, ?>> map, Tables tables, DdlParser ddlParser) {

   }

   @Override public void stop() {

   }

   @Override public boolean exists() {
      return false;
   }

   @Override public boolean storageExists() {
      return false;
   }

   @Override public void initializeStorage() {

   }

   @Override public boolean storeOnlyCapturedTables() {
      return false;
   }

   @Override public boolean skipUnparseableDdlStatements() {
      return false;
   }
}
