package io.gingersnapproject.metrics;

import io.debezium.connector.oracle.OracleStreamingChangeEventSourceMetricsMXBean;

import javax.management.MalformedObjectNameException;

/**
 * Lookup Oracle Database {@link  OracleStreamingChangeEventSourceMetricsMXBean} in JMX registry.
 */
public class OracleStreamingBeanLookup extends BaseStreamingBeanLookup<OracleStreamingChangeEventSourceMetricsMXBean> {
   public OracleStreamingBeanLookup(String rule) throws MalformedObjectNameException {
      super("oracle", rule);
   }

   @Override
   Class<OracleStreamingChangeEventSourceMetricsMXBean> mbeanClass() {
      return OracleStreamingChangeEventSourceMetricsMXBean.class;
   }
}
