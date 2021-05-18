package org.folio.eusage.reports;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

public interface TenantInit {
  Future<Void> postInit(Vertx vertx, String tenant, JsonObject tenantAttributes);

  Future<Void> preInit(Vertx vertx, String tenant, JsonObject tenantAttributes);

}
