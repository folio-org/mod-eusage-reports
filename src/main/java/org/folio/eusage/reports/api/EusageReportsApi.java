package org.folio.eusage.reports.api;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.parsetools.JsonParser;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.ext.web.openapi.RouterBuilder;
import io.vertx.ext.web.validation.RequestParameters;
import io.vertx.ext.web.validation.ValidationHandler;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowStream;
import io.vertx.sqlclient.Tuple;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.tlib.RouterCreator;
import org.folio.tlib.TenantInitHooks;
import org.folio.tlib.postgres.TenantPgPool;

public class EusageReportsApi implements RouterCreator, TenantInitHooks {
  private final Logger log = LogManager.getLogger(EusageReportsApi.class);

  String version;
  WebClient webClient;

  public EusageReportsApi(String version) {
    this.version = version;
  }

  static String teTable(TenantPgPool pool) {
    return pool.getSchema() + ".te_table";
  }

  static void failHandler(int statusCode, RoutingContext ctx, Throwable e) {
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
    return pool.getConnection()
        .compose(sqlConnection ->
            sqlConnection.prepare("SELECT * FROM " + teTable(pool))
                .compose(pq ->
                    sqlConnection.begin().compose(tx -> {
                      ctx.response().setChunked(true);
                      ctx.response().putHeader("Content-Type", "application/json");
                      ctx.response().write("{ \"titles\" : [");
                      AtomicInteger offset = new AtomicInteger();
                      RowStream<Row> stream = pq.createStream(50);
                      stream.handler(row -> {
                        if (offset.incrementAndGet() > 1) {
                          ctx.response().write(",");
                        }
                        ctx.response().write(new JsonObject()
                            .put("id", row.getUUID(0))
                            .put("counterReportTitle", row.getString(1))
                            .encode());
                      });
                      stream.endHandler(end -> {
                        ctx.response().write("] }");
                        ctx.response().end();
                        tx.commit();
                      });
                      return Future.succeededFuture();
                    })
                ));
  }

  Future<Void> populateCounterReportTitles(Vertx vertx, RoutingContext ctx) {
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    String tenant = params.headerParameter(XOkapiHeaders.TENANT).getString();
    String okapiUrl = params.headerParameter(XOkapiHeaders.URL).getString();
    String token = params.headerParameter(XOkapiHeaders.TOKEN).getString();

    Promise<Void> promise = Promise.promise();
    JsonParser parser = JsonParser.newParser();
    parser.handler(event -> log.info("obj={}", event.objectValue().encodePrettily()));
    parser.endHandler(e -> promise.complete());
    return webClient.getAbs(okapiUrl + "/counter-reports")
        .putHeader(XOkapiHeaders.TOKEN, token)
        .putHeader(XOkapiHeaders.TENANT, tenant)
        .as(BodyCodec.jsonStream(parser))
        .send()
        .compose(res -> {
          if (res.statusCode() != 200) {
            return Future.failedFuture("Bad status code {} for /counter-reports");
          }
          return promise.future();
        });
  }

  @Override
  public Future<Router> createRouter(Vertx vertx) {
    webClient = WebClient.create(vertx);
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
              .handler(ctx -> getReportTitles(vertx, ctx)
                  .onFailure(cause -> failHandler(400, ctx, cause)));
          return Future.succeededFuture(routerBuilder.createRouter());
        });
  }

  boolean lookupTenantParameter(JsonObject tenantAttributes, String key) {
    JsonArray parameters = tenantAttributes.getJsonArray("parameters");
    if (parameters != null) {
      for (int i = 0; i < parameters.size(); i++) {
        JsonObject o = parameters.getJsonObject(i);
        if (key.equals(o.getString("key")) && "true".equals(o.getString("value"))) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public Future<Void> postInit(Vertx vertx, String tenant, JsonObject tenantAttributes) {
    if (!tenantAttributes.containsKey("module_to")) {
      return Future.succeededFuture(); // doing nothing for disable
    }
    TenantPgPool pool = TenantPgPool.pool(vertx, tenant);
    Future<Void> future = pool
        .query("CREATE TABLE IF NOT EXISTS " + teTable(pool) + " ( "
            + "id UUID PRIMARY KEY, "
            + "counterReportTitle text, "
            + "counterReportId UUID, "
            + "kbTitleName text, "
            + "kbTitleId UUID, "
            + "kbPackageName text, "
            + "kbPackageId UUID, "
            + "kbManualMatch boolean"
            + ")")
        .execute().mapEmpty();
    if (lookupTenantParameter(tenantAttributes, "loadSample")) {
      for (int j = 0; j < 100; j++) {
        final var titleId = j;
        future = future.compose(res ->
            pool.preparedQuery("INSERT INTO " + teTable(pool)
                + "(id, counterReportTitle, counterReportId, kbTitleName,"
                + " kbTitleId, kbPackageName, kbPackageId, kbManualMatch)"
                + " VALUES ($1, $2, $3, $4, $5, $6, $7, $8)")
                .execute(Tuple.tuple(List.of(UUID.randomUUID(),
                    "counter title " + titleId, UUID.randomUUID(),
                    "kb title name " + titleId, UUID.randomUUID(),
                    "kb package name " + titleId, UUID.randomUUID(),
                    false)))
                .mapEmpty()
        );
      }
    }
    return future;
  }

  @Override
  public Future<Void> preInit(Vertx vertx, String tenant, JsonObject tenantAttributes) {
    return Future.succeededFuture();
  }
}
