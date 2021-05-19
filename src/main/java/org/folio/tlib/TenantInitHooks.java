package org.folio.tlib;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

public interface TenantInitHooks {
  Future<Void> postInit(Vertx vertx, String tenant, JsonObject tenantAttributes);

  Future<Void> preInit(Vertx vertx, String tenant, JsonObject tenantAttributes);

}
