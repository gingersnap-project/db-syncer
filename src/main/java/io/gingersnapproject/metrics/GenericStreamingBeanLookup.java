package io.gingersnapproject.metrics;

import io.debezium.pipeline.metrics.StreamingChangeEventSourceMetricsMXBean;

import javax.management.MalformedObjectNameException;

import io.gingersnapproject.cdc.configuration.Database;

/**
 * Lookup the {@link StreamingChangeEventSourceMetricsMXBean} instance (common to all connectors).
 */
public class GenericStreamingBeanLookup extends BaseStreamingBeanLookup<StreamingChangeEventSourceMetricsMXBean> {

   public GenericStreamingBeanLookup(String type, String rule, Database database) throws MalformedObjectNameException {
      super(type, rule, database);
   }

   @Override
   Class<StreamingChangeEventSourceMetricsMXBean> mbeanClass() {
      return StreamingChangeEventSourceMetricsMXBean.class;
   }
}
