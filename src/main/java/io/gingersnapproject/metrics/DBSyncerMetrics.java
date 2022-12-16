package io.gingersnapproject.metrics;

/**
 * Metrics interface.
 * <p>
 * All component that require storing metrics should interact with this interface.
 */
public interface DBSyncerMetrics {

   <T> CacheServiceAccessRecord<T> recordCacheServicePut();

   <T> CacheServiceAccessRecord<T> recordCacheServiceRemove();

}
