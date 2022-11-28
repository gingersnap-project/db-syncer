package io.gingersnapproject.cdc.search.opensearch;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.client.RestClient;
import org.infinispan.commons.dataconversion.internal.Json;
import org.jboss.logging.Logger;

import io.gingersnapproject.cdc.SearchBackend;
import io.gingersnapproject.cdc.translation.ColumnStringTranslator;
import io.gingersnapproject.cdc.translation.JsonTranslator;
import io.gingersnapproject.cdc.translation.PrependStringTranslator;

public class OpenSearchIndexBackend implements SearchBackend {

   private static final Logger LOG = Logger.getLogger(OpenSearchIndexBackend.class);

   final RestClient restClient;
   final String indexName;
   final JsonTranslator<StringBuilder> keyTranslator;
   final JsonTranslator<?> valueTranslator;

   public OpenSearchIndexBackend(RestClient restClient, String indexName, JsonTranslator<?> keyTranslator,
                                 JsonTranslator<?> valueTranslator) {
      this.indexName = indexName;
      this.restClient = restClient;

      // TODO Support json keys
      if (keyTranslator instanceof PrependStringTranslator && keyTranslator instanceof ColumnStringTranslator) {
         this.keyTranslator = (JsonTranslator<StringBuilder>) keyTranslator;
      } else {
         throw new UnsupportedOperationException("json keys are not supported at the moment");
      }

      this.valueTranslator = valueTranslator;

      // perform the init mapping synchronously
      mapping();
   }

   @Override
   public CompletionStage<Void> remove(Json json) {
      String key = keyTranslator.apply(json).toString();

      Request request = new Request(
            "DELETE",
            "/" + indexName + "/_doc/" + key);

      return submit(request);
   }

   @Override
   public CompletionStage<Void> put(Json json) {
      String key = keyTranslator.apply(json).toString();
      String value = valueTranslator.apply(json).toString();

      Request request = new Request(
            "PUT",
            "/" + indexName + "/_doc/" + key);
      request.setJsonEntity(value);

      return submit(request);
   }

   private void mapping() {
      Request request = new Request("PUT", "/" + indexName);

      Json mappings = Json.object("mappings")
            .set("_source", Json.object()
                  .set("enabled", false));

      request.setJsonEntity(mappings.toString());

      try {
         Response response = restClient.performRequest(request);

         LOG.info("Index created: " + EntityUtils.toString(response.getEntity()));
      } catch (ResponseException ex) {
         LOG.info("Index already present: " + ex.getMessage());
      } catch (IOException ex) {
         throw new RuntimeException(ex);
      }
   }

   private final CompletionStage<Void> submit(Request request) {
      OpenSearchResponseListener responseListener = new OpenSearchResponseListener();
      restClient.performRequestAsync(request, responseListener);
      return responseListener.completionStage();
   }

   private static class OpenSearchResponseListener implements ResponseListener {
      CompletableFuture<Void> future = new CompletableFuture<>();

      @Override
      public void onSuccess(Response response) {
         future.complete(null);
      }

      @Override
      public void onFailure(Exception exception) {
         if ( exception instanceof ResponseException ) {
            /*
             * The client tries to guess what's an error and what's not, but it's too naive.
             * A 404 on DELETE is not always important to us, for instance.
             * Thus we ignore the exception and do our own checks afterwards.
             */
            future.complete( null );
         }
         future.completeExceptionally(exception);
      }

      public CompletionStage<Void> completionStage() {
         return future;
      }
   }
}
