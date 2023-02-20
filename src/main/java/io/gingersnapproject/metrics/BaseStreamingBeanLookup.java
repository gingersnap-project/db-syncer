package io.gingersnapproject.metrics;

import java.lang.invoke.MethodHandles;
import java.lang.management.ManagementFactory;
import java.util.Hashtable;
import java.util.function.Supplier;

import javax.management.JMX;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import io.gingersnapproject.cdc.configuration.Database;
import io.gingersnapproject.cdc.connector.DatabaseProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.pipeline.metrics.StreamingChangeEventSourceMetricsMXBean;

/**
 * Finds the MBean to collect metrics from the connector.
 *
 * @param <T> The MBean type.
 */
abstract class BaseStreamingBeanLookup<T extends StreamingChangeEventSourceMetricsMXBean> implements Supplier<T> {

   private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private final ObjectName objectName;
   private volatile T bean;

   public BaseStreamingBeanLookup(String type, String rule) throws MalformedObjectNameException {
      this(type, rule, null);
   }

   public BaseStreamingBeanLookup(String type, String rule, Database database) throws MalformedObjectNameException {
      objectName = new ObjectName("debezium." + type, jmxTypes(rule, database));
   }

   private static Hashtable<String, String> jmxTypes(String rule, Database database) {
      var table = new Hashtable<String, String>();
      table.put("context", "streaming");
      table.put("type", "connector-metrics");
      table.put("server", rule);
      if (database != null && database.type().equals(DatabaseProvider.SQLSERVER)) {
         table.put("task", "0");
         database.database().ifPresent(d -> table.put("database", d));
      }
      return table;
   }

   @Override
   public T get() {
      if (bean != null) {
         return bean;
      }
      MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
      if (mBeanServer == null) {
         return null;
      }
      synchronized (this) {
         if (bean != null) {
            return bean;
         }
         if (mBeanServer.isRegistered(objectName)) {
            bean = JMX.newMBeanProxy(mBeanServer, objectName, mbeanClass());
         } else {
            log.warn("Failed to find JMX MBean {}. Connector metrics are not available", objectName);
         }
         return bean;
      }
   }

   abstract Class<T> mbeanClass();
}
