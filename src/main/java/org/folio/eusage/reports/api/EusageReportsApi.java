package org.folio.eusage.reports.api;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.openapi.RouterBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.tlib.RouterCreator;
import org.folio.tlib.TenantInitHooks;

public class EusageReportsApi implements RouterCreator, TenantInitHooks {
  final Logger log = LogManager.getLogger(EusageReportsApi.class);

  String version;

  public EusageReportsApi(String version) {
    this.version = version;
  }

  void failHandler(RoutingContext ctx) {
    ctx.response().setStatusCode(400);
    ctx.response().putHeader("Content-Type", "text/plain");
    ctx.response().end("Failure");
  }

  @Override
  public Future<Router> createRouter(Vertx vertx) {
    return RouterBuilder.create(vertx, "openapi/eusage-reports-1.0.yaml")
        .compose(routerBuilder -> {
          routerBuilder
              .operation("getVersion")
              .handler(ctx -> {
                log.info("getVersion handler");
                ctx.response().setStatusCode(200);
                ctx.response().putHeader("Content-Type", "text/plain");
                ctx.response().end(version == null ? "0.0" : version);
              })
              .failureHandler(this::failHandler);
          return Future.succeededFuture(routerBuilder.createRouter());
        });
  }

  @Override
  public Future<Void> postInit(Vertx vertx, String tenant, JsonObject tenantAttributes) {
    return Future.succeededFuture();
  }

  @Override
  public Future<Void> preInit(Vertx vertx, String tenant, JsonObject tenantAttributes) {
    return Future.succeededFuture();
  }
}
