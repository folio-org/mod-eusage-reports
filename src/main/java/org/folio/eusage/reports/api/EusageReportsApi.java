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
import io.vertx.ext.web.validation.RequestParameter;
import io.vertx.ext.web.validation.RequestParameters;
import io.vertx.ext.web.validation.ValidationHandler;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowStream;
import io.vertx.sqlclient.Tuple;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.okapi.common.GenericCompositeFuture;
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

  static String tdTable(TenantPgPool pool) {
    return pool.getSchema() + ".td_table";
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
    return populateCounterReportTitles(vertx, ctx)
        .compose(x -> returnReportTitles(vertx, ctx));
  }

  Future<Void> returnReportTitles(Vertx vertx, RoutingContext ctx) {
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    String tenant = stringOrNull(params.headerParameter(XOkapiHeaders.TENANT));

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

  Future<UUID> upsertTeEntry(TenantPgPool tenantPgPool, String counterReportTitle, String match) {
    return tenantPgPool.getConnection().compose(con -> con.begin()
        .compose(tx ->
            con.preparedQuery("SELECT id FROM " + teTable(tenantPgPool)
                + " WHERE counterReportTitle = $1")
                .execute(Tuple.of(counterReportTitle))
                .compose(res1 -> {
                  if (res1.iterator().hasNext()) {
                    return Future.succeededFuture(res1.iterator().next().getUUID(0));
                  }
                  // TODO: lookup KB here
                  return con.preparedQuery("INSERT INTO " + teTable(tenantPgPool)
                      + "(id, counterReportTitle, matchCriteria,"
                      + " kbTitleName, kbTitleId,"
                      + " kbPackageName, kbPackageId,"
                      + " kbManualMatch)"
                      + " VALUES ($1, $2, $3, $4, $5, $6, $7, $8)"
                      + " ON CONFLICT (counterReportTitle) DO NOTHING")
                      .execute(Tuple.tuple(List.of(UUID.randomUUID(), counterReportTitle, match,
                          "", UUID.randomUUID(),
                          "", UUID.randomUUID(),
                          false)))
                      .compose(x ->
                          con.preparedQuery("SELECT id FROM " + teTable(tenantPgPool)
                              + " WHERE counterReportTitle = $1")
                              .execute(Tuple.of(counterReportTitle))
                              .map(res2 -> res2.iterator().next().getUUID(0))
                      );
                })
                .compose(id -> {
                  tx.commit();
                  return Future.succeededFuture(id);
                })
        )
        .eventually(x -> con.close()));
  }

  Future<Void> upsertTdEntry(TenantPgPool tenantPgPool, UUID reportTitleId, String match) {
    return Future.succeededFuture();
  }

  Future<Void> handleReport(TenantPgPool tenantPgPool, JsonObject jsonObject) {
    Future<Void> future = Future.succeededFuture();
    JsonArray customers = jsonObject.getJsonObject("report").getJsonArray("customer");
    for (int i = 0; i < customers.size(); i++) {
      JsonObject customer = customers.getJsonObject(i);
      JsonArray reportItems = customer.getJsonArray("reportItems");
      for (int j = 0; j < reportItems.size(); j++) {
        JsonObject reportItem = reportItems.getJsonObject(j);
        final String counterReportTitle = reportItem.getString("itemName");
        JsonArray itemIdentifiers = reportItem.getJsonArray("itemIdentifier");
        String issn = null;
        for (int k = 0; k < itemIdentifiers.size(); k++) {
          JsonObject itemIdentifier = itemIdentifiers.getJsonObject(k);
          String type = itemIdentifier.getString("type");
          String value = itemIdentifier.getString("value");
          if ("ONLINE_ISSN".equals(type)) {
            issn = value;
          }
        }
        final String match = issn;
        future = future.compose(x ->
          upsertTeEntry(tenantPgPool, counterReportTitle, match)
              .compose(reportTitleId -> upsertTdEntry(tenantPgPool, reportTitleId, match))
        );
      }
    }
    return future;
  }

  static String stringOrNull(RequestParameter requestParameter) {
    return requestParameter == null ? null : requestParameter.getString();
  }

  Future<Void> populateCounterReportTitles(Vertx vertx, RoutingContext ctx) {
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    log.info("params={}", params.toJson());
    final String tenant = stringOrNull(params.headerParameter(XOkapiHeaders.TENANT));
    final String okapiUrl = stringOrNull(params.headerParameter(XOkapiHeaders.URL));
    final String token = stringOrNull(params.headerParameter(XOkapiHeaders.TOKEN));

    if (okapiUrl == null) {
      return Future.failedFuture("Missing " + XOkapiHeaders.URL);
    }
    Promise<Void> promise = Promise.promise();
    JsonParser parser = JsonParser.newParser();
    AtomicBoolean objectMode = new AtomicBoolean(false);
    TenantPgPool pool = TenantPgPool.pool(vertx, tenant);
    List<Future<Void>> futures = new LinkedList<>();

    parser.handler(event -> {
      log.info("event type={}", event.type().name());
      if (objectMode.get() && event.isObject()) {
        JsonObject obj = event.objectValue();
        log.info("Object value {}", obj.encodePrettily());
        futures.add(handleReport(pool, obj));
      } else {
        String f = event.fieldName();
        if ("counterReports".equals(f)) {
          objectMode.set(true);
          parser.objectValueMode();
        }
      }
    });
    parser.exceptionHandler(x -> log.error("parser.exceptionHandler {}", x.getMessage(), x));
    parser.endHandler(e -> {
      log.error("parser.endHandler");
      GenericCompositeFuture.all(futures)
          .onComplete(x -> promise.handle(x.mapEmpty()));
    });
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
            + "counterReportTitle text UNIQUE, "
            + "matchCriteria text, "
            + "kbTitleName text, "
            + "kbTitleId UUID, "
            + "kbPackageName text, "
            + "kbPackageId UUID, "
            + "kbManualMatch boolean"
            + ")")
        .execute().mapEmpty();
    future = future.compose(x -> pool
        .query("CREATE TABLE IF NOT EXISTS " + tdTable(pool) + " ( "
            + "id UUID PRIMARY KEY, "
            + "reportTitleId UUID, "
            + "counterReportId UUID UNIQUE, "
            + "pubYear text, "
            + "usageYearMonth text, "
            + "uniqueAccessCount integer, "
            + "totalAccessCount integer, "
            + "openAccess boolean"
            + ")")
        .execute().mapEmpty());
    if (lookupTenantParameter(tenantAttributes, "loadSample")) {
      for (int j = 0; j < 100; j++) {
        final var titleId = j;
        future = future.compose(res ->
            pool.preparedQuery("INSERT INTO " + teTable(pool)
                + "(id, counterReportTitle, matchCriteria, kbTitleName,"
                + " kbTitleId, kbPackageName, kbPackageId, kbManualMatch)"
                + " VALUES ($1, $2, $3, $4, $5, $6, $7, $8)")
                .execute(Tuple.tuple(List.of(UUID.randomUUID(),
                    "counter title " + titleId, "123" + titleId,
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
