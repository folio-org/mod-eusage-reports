package org.folio.eusage.reports.api;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.openapi.RouterBuilder;
import io.vertx.ext.web.validation.RequestParameters;
import io.vertx.ext.web.validation.ValidationHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.tlib.RouterCreator;
import org.folio.tlib.TenantInitHooks;
import org.folio.tlib.postgres.TenantPgPool;

public class EusageReportsApi implements RouterCreator, TenantInitHooks {
  private static final String TE_TABLE = "{schema}.te_table";

  private final Logger log = LogManager.getLogger(EusageReportsApi.class);

  String version;

  public EusageReportsApi(String version) {
    this.version = version;
  }

  void failHandler(int statusCode, RoutingContext ctx, Throwable e) {
    ctx.response().setStatusCode(statusCode);
    ctx.response().putHeader("Content-Type", "text/plain");
    String msg = null;
    if (e != null) {
      msg = e.getMessage();
    }
    if (msg == null) {
      msg = "Failure";
    }
    ctx.response().end(msg);
  }

  Future<Void> getReportTitles(Vertx vertx, RoutingContext ctx) {
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    String tenant = params.headerParameter(XOkapiHeaders.TENANT).getString();

    TenantPgPool pool = TenantPgPool.pool(vertx, tenant);
    return pool.query("SELECT * FROM " + TE_TABLE)
        .execute()
        .compose(rowSet -> {
          ctx.response().setStatusCode(200);
          ctx.response().putHeader("Content-Type", "application/json");
          // TODO: mapping
          ctx.end("{ \"titles\": [ ] }");
          return Future.succeededFuture();
        });
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
              .failureHandler(ctx -> failHandler(400, ctx, null));
          routerBuilder
              .operation("getReportTitles")
              .handler(ctx -> {
                getReportTitles(vertx, ctx)
                    .onFailure(cause -> failHandler(400, ctx, cause));
              });
          return Future.succeededFuture(routerBuilder.createRouter());
        });
  }

  @Override
  public Future<Void> postInit(Vertx vertx, String tenant, JsonObject tenantAttributes) {
    if (!tenantAttributes.containsKey("module_to")) {
      return Future.succeededFuture(); // doing nothing for disable
    }
    TenantPgPool pool = TenantPgPool.pool(vertx, tenant);
    return pool
        .query("CREATE TABLE IF NOT EXISTS " + TE_TABLE + " ( "
            + "id UUID PRIMARY KEY, "
            + "counterReportTitle text, "
            + "counterReportId UUID, "
            + "kbTitleName text, "
            + "kbTitleId UUID, "
            + "kbPackageName text, "
            + "kbPackageId UUID, "
            + "kbManualMatch boolean"
            + ")")
        .execute()
        .mapEmpty();
  }

  @Override
  public Future<Void> preInit(Vertx vertx, String tenant, JsonObject tenantAttributes) {
    return Future.succeededFuture();
  }
}
