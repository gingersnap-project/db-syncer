package io.gingersnapproject.metrics;

import io.debezium.connector.mysql.Module;
import io.debezium.connector.mysql.MySqlStreamingChangeEventSourceMetricsMXBean;

import javax.management.MalformedObjectNameException;

/**
 * Lookup the {@link MySqlStreamingChangeEventSourceMetricsMXBean} instance.
 */
public class MySQLStreamingBeanLookup extends BaseStreamingBeanLookup<MySqlStreamingChangeEventSourceMetricsMXBean> {

   public MySQLStreamingBeanLookup(String rule) throws MalformedObjectNameException {
      super(Module.name(), rule);
   }

   @Override
   Class<MySqlStreamingChangeEventSourceMetricsMXBean> mbeanClass() {
      return MySqlStreamingChangeEventSourceMetricsMXBean.class;
   }
}
