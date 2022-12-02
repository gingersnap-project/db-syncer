package io.gingersnapproject.cdc.cache.hotrod;

import java.net.URI;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.inject.Inject;
import javax.inject.Singleton;

import io.gingersnapproject.cdc.CacheBackend;
import io.gingersnapproject.cdc.OffsetBackend;
import io.gingersnapproject.cdc.SchemaBackend;
import io.gingersnapproject.cdc.cache.CacheService;
import io.gingersnapproject.cdc.configuration.Backend;
import io.gingersnapproject.cdc.configuration.Rule;
import io.gingersnapproject.cdc.translation.ColumnJsonTranslator;
import io.gingersnapproject.cdc.translation.ColumnStringTranslator;
import io.gingersnapproject.cdc.translation.IdentityTranslator;
import io.gingersnapproject.cdc.event.NotificationManager;
import io.gingersnapproject.cdc.translation.JsonTranslator;
import io.gingersnapproject.cdc.translation.PrependJsonTranslator;
import io.gingersnapproject.cdc.translation.PrependStringTranslator;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.multimap.MultimapCacheManager;
import org.infinispan.client.hotrod.multimap.RemoteMultimapCacheManagerFactory;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.commons.configuration.StringConfiguration;
import org.infinispan.commons.dataconversion.MediaType;

import io.quarkus.arc.lookup.LookupIfProperty;

@LookupIfProperty(name = "service.hotrod.enabled", stringValue = "true", lookupIfMissing = true)
@Singleton
public class HotRodService implements CacheService {

   @Inject NotificationManager eventing;
   ConcurrentMap<URI, RuleCacheManager> otherURIs = new ConcurrentHashMap<>();
   private static final String OFFSET_CACHE_NAME = "debezium-offset";
   private static final String SCHEMA_CACHE_NAME = "debezium-schema";

   @Override
   public boolean supportsURI(URI uri) {
      return uri.getScheme().startsWith("hotrod");
   }

   @Override
   public CompletionStage<Boolean> reconnect(URI uri) {
      RuleCacheManager rcm = otherURIs.get(uri);
      if (rcm == null) {
         throw new IllegalStateException("URI " + uri + " not present to restart!");
      }
      return rcm.manager.startAsync().handle((__, t) -> {
         boolean successful = t == null;
         if (successful) {
            createCaches(rcm.rule, rcm.manager);
            eventing.backendStartedEvent(uri);
         }
         return successful;
      });
   }

   @Override
   public CompletionStage<Void> stop(URI uri) {
      RuleCacheManager rcm = otherURIs.get(uri);
      if (rcm == null) {
         throw new IllegalStateException("URI " + uri + " not present to stop!");
      }
      return rcm.manager.stopAsync();
   }

   @Override
   public CacheBackend backendForRule(String name, Rule.SingleRule rule) {
      Backend backend = rule.backend();
      JsonTranslator<?> keyTranslator;
      JsonTranslator<?> valueTranslator = backend.columns().isPresent() ?
            new ColumnJsonTranslator(backend.columns().get()) : IdentityTranslator.getInstance();
      Optional<List<String>> optionalKeys = backend.keyColumns();
      // TODO: hardcoded value here
      List<String> columnsToUse = optionalKeys.orElse(List.of("id"));
      switch (backend.keyType()) {
         case PLAIN -> {
            ColumnStringTranslator stringTranslator = new ColumnStringTranslator(columnsToUse,
                  backend.plainSeparator());
            keyTranslator = backend.prefixRuleName() ?
                  new PrependStringTranslator(stringTranslator, name) :
                  stringTranslator;
         }
         case JSON -> {
            ColumnJsonTranslator jsonTranslator = new ColumnJsonTranslator(columnsToUse);
            // TODO: hardcoded value here
            keyTranslator = backend.prefixRuleName() ?
                  new PrependJsonTranslator(jsonTranslator, backend.jsonRuleName(), name) :
                  jsonTranslator;
         }
         default -> throw new IllegalArgumentException("Key type: " + backend.keyType() + " not supported!");
      }
      return backendForRule(name, backend.uri(), keyTranslator, valueTranslator);
   }

   private RuleCacheManager getManagerFor(String name, URI uri) {
      return otherURIs.computeIfAbsent(uri, ignore -> {
         try {
            RemoteCacheManager remoteCacheManager = new RemoteCacheManager(uri);
            createCaches(name, remoteCacheManager);
            eventing.backendStartedEvent(uri);
            return new RuleCacheManager(name, remoteCacheManager);
         } catch (Exception e) {
            eventing.backendFailedEvent(uri, e);
            return null;
         }
      });
   }

   private RemoteCacheManager managerForURI(URI uri) {
      RuleCacheManager rcm = Objects.requireNonNull(otherURIs.get(uri), "Manager for " + uri + " was not created yet");
      return rcm.manager;
   }

   private CacheBackend backendForRule(String name, URI uri, JsonTranslator<?> keyTranslator, JsonTranslator<?> valueTranslator) {
      RuleCacheManager rcm = getManagerFor(name, uri);
      if (rcm == null) return null;
      return new HotRodCacheBackend(uri, rcm.manager.getCache(name), keyTranslator, valueTranslator, eventing);
   }

   private void getOrCreateCacheBackendCache(String name, RemoteCacheManager rcm) {
      rcm.administration()
            .withFlags(CacheContainerAdmin.AdminFlag.VOLATILE)
            .getOrCreateCache(name, new StringConfiguration(
            "<local-cache>" +
               "<encoding>" +
                  "<key media-type=\"" + MediaType.TEXT_PLAIN_TYPE + "\"/>" +
                  "<value media-type=\"" + MediaType.TEXT_PLAIN_TYPE + "\"/>" +
               "</encoding>" +
            "</local-cache>"));
   }

   @Override
   public OffsetBackend offsetBackendForURI(URI uri) {
      RemoteCacheManager remoteCacheManager = managerForURI(uri);
      if (remoteCacheManager == null) return null;
      return new HotRodOffsetBackend(remoteCacheManager.getCache(OFFSET_CACHE_NAME));
   }

   private void getOrCreateOffsetBackendCache(String name, RemoteCacheManager rcm) {
      rcm.administration()
            .withFlags(CacheContainerAdmin.AdminFlag.VOLATILE)
            .getOrCreateCache(name, new StringConfiguration(
            "<local-cache>" +
               "<encoding media-type=\"" + MediaType.APPLICATION_OCTET_STREAM_TYPE + "\"/>" +
            "</local-cache>"));
   }

   @Override
   public SchemaBackend schemaBackendForURI(URI uri) {
      RemoteCacheManager remoteCacheManager = managerForURI(uri);
      if (remoteCacheManager == null) return null;
      getOrCreateSchemaBackendCache(SCHEMA_CACHE_NAME, remoteCacheManager);
      MultimapCacheManager<String, String> remoteMultimapCacheManager = RemoteMultimapCacheManagerFactory.from(remoteCacheManager);
      // Support duplicates so it uses a list which is ordered
      return new HotRodSchemaBackend(remoteMultimapCacheManager.get(SCHEMA_CACHE_NAME, true));
   }

   private RemoteCache<String, String> getOrCreateSchemaBackendCache(String multiMapCacheName, RemoteCacheManager rcm) {
      return rcm.administration()
            .withFlags(CacheContainerAdmin.AdminFlag.VOLATILE)
            .getOrCreateCache(multiMapCacheName, new StringConfiguration(
            "<local-cache>" +
               "<encoding>" +
                  "<key media-type=\"" + MediaType.TEXT_PLAIN_TYPE + "\"/>" +
                  "<value media-type=\"" + MediaType.APPLICATION_OCTET_STREAM_TYPE + "\"/>" +
               "</encoding>" +
            "</local-cache>"));
   }

   private void createCaches(String ruleName, RemoteCacheManager remoteCacheManager) {
      getOrCreateCacheBackendCache(ruleName, remoteCacheManager);
      getOrCreateOffsetBackendCache(OFFSET_CACHE_NAME, remoteCacheManager);
      getOrCreateSchemaBackendCache(SCHEMA_CACHE_NAME, remoteCacheManager);
   }

   @Override
   public void shutdown(URI uri) {
      RuleCacheManager rcm = otherURIs.remove(uri);
      if (rcm != null) {
         rcm.manager.stopAsync().whenComplete((ignore, t) -> {
            if (t != null) {
               eventing.backendFailedEvent(uri, t);
            } else {
               eventing.backendStoppedEvent(uri);
            }
         });
      }
   }

   private record RuleCacheManager(String rule, RemoteCacheManager manager) { }
}
