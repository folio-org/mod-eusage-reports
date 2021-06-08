package org.folio.eusage.reports.api;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.RequestOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.parsetools.JsonEventType;
import io.vertx.core.parsetools.JsonParser;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.ext.web.openapi.RouterBuilder;
import io.vertx.ext.web.validation.RequestParameter;
import io.vertx.ext.web.validation.RequestParameters;
import io.vertx.ext.web.validation.ValidationHandler;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowStream;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.Tuple;
import java.util.Deque;
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

  private WebClient webClient;

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

  static String stringOrNull(RequestParameter requestParameter) {
    return requestParameter == null ? null : requestParameter.getString();
  }

  Future<Void> getReportTitles(Vertx vertx, RoutingContext ctx) {
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    String tenant = stringOrNull(params.headerParameter(XOkapiHeaders.TENANT));

    TenantPgPool pool = TenantPgPool.pool(vertx, tenant);
    return pool.getConnection()
        .compose(sqlConnection ->
            sqlConnection.prepare("SELECT * FROM " + teTable(pool))
                .<Void>compose(pq ->
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
                        JsonObject response = new JsonObject()
                            .put("id", row.getUUID(0))
                            .put("counterReportTitle", row.getString(1));
                        String titleName = row.getString(3);
                        if (titleName != null) {
                          response.put("kbTitleName", titleName)
                              .put("kbTitleId", row.getUUID(4))
                              .put("kbPackageName", row.getString(5))
                              .put("kbPackageId", row.getUUID(6));
                        }
                        ctx.response().write(response.encode());
                      });
                      stream.endHandler(end -> {
                        ctx.response().write("] }");
                        ctx.response().end();
                        tx.commit();
                      });
                      return Future.succeededFuture();
                    })
                )
                .eventually(x -> sqlConnection.close())
        );
  }

  Future<Void> getTitleData(Vertx vertx, RoutingContext ctx) {
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    String tenant = stringOrNull(params.headerParameter(XOkapiHeaders.TENANT));

    TenantPgPool pool = TenantPgPool.pool(vertx, tenant);
    return pool.getConnection()
        .compose(sqlConnection ->
            sqlConnection.prepare("SELECT * FROM " + tdTable(pool))
                .<Void>compose(pq ->
                    sqlConnection.begin().compose(tx -> {
                      ctx.response().setChunked(true);
                      ctx.response().putHeader("Content-Type", "application/json");
                      ctx.response().write("{ \"data\" : [");
                      AtomicInteger offset = new AtomicInteger();
                      RowStream<Row> stream = pq.createStream(50);
                      stream.handler(row -> {
                        if (offset.incrementAndGet() > 1) {
                          ctx.response().write(",");
                        }
                        JsonObject obj = new JsonObject()
                            .put("id", row.getUUID(0))
                            .put("reportTitleId", row.getUUID(1))
                            .put("counterReportId", row.getUUID(2))
                            .put("pubYear", row.getString(3))
                            .put("usageYearMonth", row.getString(4))
                            .put("uniqueAccessCount", row.getInteger(5))
                            .put("totalAccessCount", row.getInteger(6))
                            .put("openAccess", row.getBoolean(7));
                        ctx.response().write(obj.encode());
                      });
                      stream.endHandler(end -> {
                        ctx.response().write("] }");
                        ctx.response().end();
                        tx.commit();
                      });
                      return Future.succeededFuture();
                    })
                )
                .eventually(x -> sqlConnection.close())
        );
  }

  Future<Void> postFromCounter(Vertx vertx, RoutingContext ctx) {
    return populateCounterReportTitles(vertx, ctx)
        .compose(x -> {
          if (Boolean.TRUE.equals(x)) {
            return getReportTitles(vertx, ctx);
          }
          ctx.response().setStatusCode(404);
          ctx.response().putHeader("Content-Type", "text/plain");
          ctx.response().end("Not found");
          return Future.succeededFuture();
        });
  }

  Future<Tuple> ermLookup(RoutingContext ctx, String identifier) {
    // assuming identifier only has unreserved characters
    String uri = "/erm/resource?match=identifiers.identifier.value&term=" + identifier;
    Future<JsonArray> future = createRequest(webClient, HttpMethod.GET, ctx, uri)
        .send()
        .compose(res -> {
          if (res.statusCode() != 200) {
            return Future.failedFuture(uri + " returned " + res.statusCode());
          }
          JsonArray ar = res.bodyAsJsonArray();
          return Future.succeededFuture(ar);
        });
    return future.compose(ar -> {
      if (ar.isEmpty()) {
        return Future.succeededFuture(null);
      }
      // TODO : there could be more than one package associated with title
      JsonObject resource = ar.getJsonObject(0);
      String titleId = resource.getString("id");
      String uri2 = "/erm/resource/" + titleId + "/entitlementOptions"
          + "?match=class&term=org.olf.kb.Pkg";
      return createRequest(webClient, HttpMethod.GET, ctx, uri2)
          .send()
          .compose(res -> {
            if (res.statusCode() != 200) {
              return Future.failedFuture(uri2 + " returned " + res.statusCode());
            }
            JsonArray ar2 = res.bodyAsJsonArray();
            if (ar2.isEmpty()) {
              return Future.succeededFuture(null);
            }
            JsonObject packageObject = ar2.getJsonObject(0);
            String kbId = packageObject.getString("id");

            return Future.succeededFuture(Tuple.of(
                UUID.fromString(titleId),
                resource.getString("name"),
                UUID.fromString(kbId),
                packageObject.getString("name")
            ));
          });
    });
  }

  Future<UUID> upsertTeEntry(TenantPgPool pool, SqlConnection con, RoutingContext ctx,
                             String counterReportTitle, String match) {
    return con.preparedQuery("SELECT id FROM " + teTable(pool)
        + " WHERE counterReportTitle = $1")
        .execute(Tuple.of(counterReportTitle))
        .compose(res1 -> {
          if (res1.iterator().hasNext()) {
            return Future.succeededFuture(res1.iterator().next().getUUID(0));
          }
          return ermLookup(ctx, match).compose(erm -> {
            Future<Void> future;
            if (erm == null) {
              future = con.preparedQuery("INSERT INTO " + teTable(pool)
                  + "(id, counterReportTitle, matchCriteria,"
                  + " kbManualMatch)"
                  + " VALUES ($1, $2, $3, $4)"
                  + " ON CONFLICT (counterReportTitle) DO NOTHING")
                  .execute(Tuple.tuple(List.of(UUID.randomUUID(), counterReportTitle, match,
                      false))).mapEmpty();
            } else {
              UUID kbTitleId = erm.getUUID(0);
              String kbTitleName = erm.getString(1);
              UUID kbPackageId = erm.getUUID(2);
              String kbPackageName = erm.getString(3);
              future = con.preparedQuery("INSERT INTO " + teTable(pool)
                  + "(id, counterReportTitle, matchCriteria,"
                  + " kbTitleName, kbTitleId,"
                  + " kbPackageName, kbPackageId,"
                  + " kbManualMatch)"
                  + " VALUES ($1, $2, $3, $4, $5, $6, $7, $8)"
                  + " ON CONFLICT (counterReportTitle) DO NOTHING")
                  .execute(Tuple.tuple(List.of(UUID.randomUUID(), counterReportTitle, match,
                      kbTitleName, kbTitleId,
                      kbPackageName, kbPackageId,
                      false))).mapEmpty();
            }
            return future.compose(x ->
                con.preparedQuery("SELECT id FROM " + teTable(pool)
                    + " WHERE counterReportTitle = $1")
                    .execute(Tuple.of(counterReportTitle))
                    .map(res2 -> res2.iterator().next().getUUID(0))
            );
          });
        });
  }

  static Future<Void> clearTdEntry(TenantPgPool pool, SqlConnection con, UUID counterReportId) {
    return con.preparedQuery("DELETE FROM " + tdTable(pool)
        + " WHERE counterReportId = $1")
        .execute(Tuple.of(counterReportId))
        .mapEmpty();
  }

  static Future<Void> clearTdEntry(TenantPgPool pool, UUID counterReportId) {
    return pool.getConnection()
        .compose(con -> clearTdEntry(pool, con, counterReportId)
            .eventually(x -> con.close()));
  }

  static Future<Void> insertTdEntry(TenantPgPool pool, SqlConnection con, UUID reportTitleId,
                                    UUID counterReportId, String usageYearMonth,
                                    int totalAccessCount) {
    return con.preparedQuery("INSERT INTO " + tdTable(pool)
        + "(id, reportTitleId,"
        + " counterReportId,"
        + " pubYear, usageYearMonth,"
        + " uniqueAccessCount, totalAccessCount, openAccess)"
        + " VALUES ($1, $2, $3, $4, $5, $6, $7, $8)")
        .execute(Tuple.tuple(List.of(UUID.randomUUID(), reportTitleId,
            counterReportId,
            "", usageYearMonth,
            totalAccessCount, totalAccessCount, false)))
        .mapEmpty();
  }

  static int getTotalCount(JsonObject reportItem) {
    int count = 0;
    JsonArray itemPerformances = reportItem.getJsonArray("itemPerformance");
    for (int i = 0; i < itemPerformances.size(); i++) {
      JsonObject itemPerformance = itemPerformances.getJsonObject(i);
      JsonArray instances = itemPerformance.getJsonArray("instance");
      for (int j = 0; j < instances.size(); j++) {
        JsonObject instance = instances.getJsonObject(j);
        count += instance.getInteger("count");
      }
    }
    return count;
  }

  static String getMatch(JsonObject reportItem) {
    JsonArray itemIdentifiers = reportItem.getJsonArray("itemIdentifier");
    for (int k = 0; k < itemIdentifiers.size(); k++) {
      JsonObject itemIdentifier = itemIdentifiers.getJsonObject(k);
      String type = itemIdentifier.getString("type");
      String value = itemIdentifier.getString("value");
      if ("ONLINE_ISSN".equals(type)) {
        return value;
      }
    }
    return null;
  }

  Future<Void> handleReport(TenantPgPool pool, RoutingContext ctx, JsonObject jsonObject) {
    final UUID counterReportId = UUID.fromString(jsonObject.getString("id"));
    final String usageYearMonth = jsonObject.getString("yearMonth");
    final JsonObject reportItem = jsonObject.getJsonObject("reportItem");

    return pool.getConnection().compose(con -> con.begin().compose(tx -> {
      Future<Void> future = Future.succeededFuture();
      final String counterReportTitle = reportItem.getString("itemName");
      final String match = getMatch(reportItem);
      final int totalAccessCount = getTotalCount(reportItem);
      future = future.compose(x ->
          upsertTeEntry(pool, con, ctx, counterReportTitle, match)
              .compose(reportTitleId -> insertTdEntry(pool, con, reportTitleId, counterReportId,
                  usageYearMonth, totalAccessCount))
      );
      return future
          .compose(x -> tx.commit())
          .eventually(x -> con.close());
    }));
  }

  static HttpRequest<Buffer> createRequest(WebClient webClient, HttpMethod method,
                                           RoutingContext ctx, String uri) {
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    final String okapiUrl = stringOrNull(params.headerParameter(XOkapiHeaders.URL));
    final String tenant = stringOrNull(params.headerParameter(XOkapiHeaders.TENANT));
    final String token = stringOrNull(params.headerParameter(XOkapiHeaders.TOKEN));
    return webClient.request(method, new RequestOptions().setAbsoluteURI(okapiUrl + uri))
        .putHeader(XOkapiHeaders.TOKEN, token)
        .putHeader(XOkapiHeaders.TENANT, tenant);
  }

  Future<Boolean> populateCounterReportTitles(Vertx vertx, RoutingContext ctx) {
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    final String tenant = stringOrNull(params.headerParameter(XOkapiHeaders.TENANT));
    final String okapiUrl = stringOrNull(params.headerParameter(XOkapiHeaders.URL));
    final String id = ctx.getBodyAsJson().getString("counterReportId");

    if (okapiUrl == null) {
      return Future.failedFuture("Missing " + XOkapiHeaders.URL);
    }
    Promise<Void> promise = Promise.promise();
    JsonParser parser = JsonParser.newParser();
    AtomicBoolean objectMode = new AtomicBoolean(false);
    TenantPgPool pool = TenantPgPool.pool(vertx, tenant);
    List<Future<Void>> futures = new LinkedList<>();

    final String uri = "/counter-reports" + (id != null ? "/" + id : "");
    JsonObject reportObj = new JsonObject();
    Deque<String> path = new LinkedList<>();
    parser.handler(event -> {
      log.debug("event type={}", event.type().name());
      JsonEventType type = event.type();
      if (JsonEventType.END_OBJECT.equals(type)) {
        path.removeLast();
        objectMode.set(false);
        parser.objectEventMode();
      }
      if (JsonEventType.START_OBJECT.equals(type)) {
        path.addLast(null);
      }
      if (objectMode.get() && event.isObject()) {
        reportObj.put("reportItem", event.objectValue());
        log.debug("Object value {}", reportObj.encodePrettily());
        futures.add(handleReport(pool, ctx, reportObj));
      } else {
        String f = event.fieldName();
        log.debug("Field = {}", f);
        if ("id".equals(f) && path.size() <= 2) {
          reportObj.put("id", event.stringValue());
          futures.add(clearTdEntry(pool, UUID.fromString(reportObj.getString("id"))));
        }
        if ("yearMonth".equals(f) && path.size() <= 2) {
          reportObj.put("yearMonth", event.stringValue());
        }
        if ("reportItems".equals(f)) {
          objectMode.set(true);
          parser.objectValueMode();
        }
      }
    });
    parser.exceptionHandler(x -> {
      log.error("GET {} returned bad JSON: {}", uri, x.getMessage(), x);
      promise.fail("GET " + uri + " returned bad JSON: " + x.getMessage());
    });
    parser.endHandler(e -> {
      log.error("parser.endHandler");
      GenericCompositeFuture.all(futures)
          .onComplete(x -> promise.handle(x.mapEmpty()));
    });
    return createRequest(webClient, HttpMethod.GET, ctx, uri)
        .as(BodyCodec.jsonStream(parser))
        .send()
        .compose(res -> {
          if (res.statusCode() == 404) {
            return Future.succeededFuture(false);
          }
          if (res.statusCode() != 200) {
            return Future.failedFuture("GET " + uri + " returned status code " + res.statusCode());
          }
          return promise.future().map(true);
        });
  }

  @Override
  public Future<Router> createRouter(Vertx vertx) {
    webClient = WebClient.create(vertx);
    return RouterBuilder.create(vertx, "openapi/eusage-reports-1.0.yaml")
        .compose(routerBuilder -> {
          routerBuilder
              .operation("getReportTitles")
              .handler(ctx -> getReportTitles(vertx, ctx)
                  .onFailure(cause -> failHandler(400, ctx, cause)));
          routerBuilder
              .operation("postFromCounter")
              .handler(ctx -> postFromCounter(vertx, ctx)
                  .onFailure(cause -> failHandler(400, ctx, cause)));
          routerBuilder
              .operation("getTitleData")
              .handler(ctx -> getTitleData(vertx, ctx)
                  .onFailure(cause -> failHandler(400, ctx, cause)));
          return Future.succeededFuture(routerBuilder.createRouter());
        });
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
            + "counterReportId UUID, "
            + "pubYear text, "
            + "usageYearMonth text, "
            + "uniqueAccessCount integer, "
            + "totalAccessCount integer, "
            + "openAccess boolean"
            + ")")
        .execute().mapEmpty());
    return future;
  }

  @Override
  public Future<Void> preInit(Vertx vertx, String tenant, JsonObject tenantAttributes) {
    return Future.succeededFuture();
  }
}
