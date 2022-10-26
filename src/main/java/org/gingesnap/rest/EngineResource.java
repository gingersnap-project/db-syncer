package org.gingesnap.rest;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

import org.gingesnap.cdc.ManagedEngine;
import org.jboss.resteasy.reactive.RestQuery;

@Path("/v1/engine")
public class EngineResource {

   @Inject ManagedEngine managedEngine;

   @GET
   @Path("{name}")
   public void enableDisableEngine(String name, @RestQuery("enable") Boolean enable) {
      if (enable) {
         managedEngine.start(name);
      } else {
         managedEngine.stop(name);
      }
   }
}
