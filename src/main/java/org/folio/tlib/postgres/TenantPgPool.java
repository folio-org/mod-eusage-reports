package org.folio.tlib.postgres;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import java.util.List;
import org.folio.tlib.postgres.impl.TenantPgPoolImpl;

public interface TenantPgPool extends PgPool {

  static TenantPgPool pool(Vertx vertx, String tenant) {
    return TenantPgPoolImpl.tenantPgPool(vertx, tenant);
  }

  Future<Void> execute(List<String> queries);

  String getSchema();

  static void setDefaultConnectOptions(PgConnectOptions connectOptions) {
    TenantPgPoolImpl.setDefaultConnectOptions(connectOptions);
  }

  static PgConnectOptions getDefaultConnectOptions() {
    return TenantPgPoolImpl.getDefaultConnectOptions();
  }

  static void setModule(String module) {
    TenantPgPoolImpl.setModule(module);
  }

  static void setServerPem(String serverPem) {
    TenantPgPoolImpl.setServerPem(serverPem);
  }

  static void setMaxPoolSize(String maxPoolSize) {
    TenantPgPoolImpl.setMaxPoolSize(maxPoolSize);
  }
}
