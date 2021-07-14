package org.folio.eusage.reports.api;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.openapi.RouterBuilder;
import io.vertx.sqlclient.Tuple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.tlib.RouterCreator;
import org.folio.tlib.postgres.TenantPgPool;
import org.folio.tlib.util.TenantUtil;

public class UseOverTimeApi implements RouterCreator {
  private static final Logger log = LogManager.getLogger(UseOverTimeApi.class);

  static String titleEntriesTable(TenantPgPool pool) {
    return pool.getSchema() + ".title_entries";
  }

  static String packageEntriesTable(TenantPgPool pool) {
    return pool.getSchema() + ".package_entries";
  }

  static String titleDataTable(TenantPgPool pool) {
    return pool.getSchema() + ".title_data";
  }

  static String agreementEntriesTable(TenantPgPool pool) {
    return pool.getSchema() + ".agreement_entries";
  }

  static void failHandler(int statusCode, RoutingContext ctx, Throwable e) {
    log.error(e.getMessage(), e);
    failHandler(statusCode, ctx, e.getMessage());
  }

  static void failHandler(int statusCode, RoutingContext ctx, String msg) {
    ctx.response().setStatusCode(statusCode);
    ctx.response().putHeader("Content-Type", "text/plain");
    ctx.response().end(msg != null ? msg : "Failure");
  }

  @Override
  public Future<Router> createRouter(Vertx vertx, WebClient webClient) {
    return RouterBuilder.create(vertx, "openapi/use-over-time-1.0.yaml")
        .map(routerBuilder -> {
          routerBuilder
              .operation("useOverTime")
              .handler(ctx -> getUseOverTime(vertx, ctx)
                  .onFailure(cause -> failHandler(400, ctx, cause)));
          return routerBuilder.createRouter();
        });
  }

  private Future<Void> getUseOverTime(Vertx vertx, RoutingContext ctx) {
    TenantPgPool pool = TenantPgPool.pool(vertx, TenantUtil.tenant(ctx));
    String agreementId = ctx.request().params().get("agreementId");
    String startDate = ctx.request().params().get("startDate");
    String endDate = ctx.request().params().get("endDate");
    pool.preparedQuery(
        "SELECT sum(COALESCE(uniqueaccesscount, 0)), min(kbtitlename), min(kbtitleid::text)"
            + " FROM " + agreementEntriesTable(pool)
            + " LEFT JOIN " + packageEntriesTable(pool) + " USING (kbpackageid, kbtitleid)"
            + " LEFT JOIN " + titleEntriesTable(pool) + " USING (kbtitleid)"
            + " LEFT JOIN " + titleDataTable(pool)
            + "    ON " + titleEntriesTable(pool) + ".id = "
            +     titleDataTable(pool) + ".titleentryid"
            + " WHERE agreementid=$1 AND daterange($2, $3) @> lower(usagedaterange)"
            + " GROUP BY kbtitleid"
        ).execute(Tuple.of(agreementId, startDate, endDate));
    return Future.succeededFuture();
  }

}
