package io.gingersnapproject.metrics;

import io.debezium.pipeline.metrics.StreamingChangeEventSourceMetricsMXBean;

import javax.management.MalformedObjectNameException;

/**
 * Lookup the {@link StreamingChangeEventSourceMetricsMXBean} instance (common to all connectors).
 */
public class GenericStreamingBeanLookup extends BaseStreamingBeanLookup<StreamingChangeEventSourceMetricsMXBean> {

   public GenericStreamingBeanLookup(String type, String rule) throws MalformedObjectNameException {
      super(type, rule);
   }

   @Override
   Class<StreamingChangeEventSourceMetricsMXBean> mbeanClass() {
      return StreamingChangeEventSourceMetricsMXBean.class;
   }
}
