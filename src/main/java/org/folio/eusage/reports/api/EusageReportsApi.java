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
import io.vertx.sqlclient.Transaction;
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

  private WebClient webClient;

  static String titleEntriesTable(TenantPgPool pool) {
    return pool.getSchema() + ".te_table";
  }

  static String titleDataTable(TenantPgPool pool) {
    return pool.getSchema() + ".td_table";
  }

  static String reportDataTable(TenantPgPool pool) {
    return pool.getSchema() + ".rd_table";
  }

  static void failHandler(int statusCode, RoutingContext ctx, Throwable e) {
    failHandler(statusCode, ctx, e != null ? e.getMessage() : null);
  }

  static void failHandler(int statusCode, RoutingContext ctx, String msg) {
    ctx.response().setStatusCode(statusCode);
    ctx.response().putHeader("Content-Type", "text/plain");
    ctx.response().end(msg != null ? msg : "Failure");
  }

  static String stringOrNull(RequestParameter requestParameter) {
    return requestParameter == null ? null : requestParameter.getString();
  }

  private static void endHandler(RoutingContext ctx, SqlConnection sqlConnection, Transaction tx) {
    ctx.response().write("] }");
    ctx.response().end();
    tx.commit().compose(x -> sqlConnection.close());
  }

  private void endStream(RowStream<Row> stream, RoutingContext ctx,
                         SqlConnection sqlConnection, Transaction tx) {
    stream.endHandler(end -> endHandler(ctx, sqlConnection, tx));
    stream.exceptionHandler(e -> {
      log.error("stream error {}", e.getMessage(), e);
      endHandler(ctx, sqlConnection, tx);
    });
  }

  Future<Void> getReportTitles(Vertx vertx, RoutingContext ctx) {
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    String tenant = stringOrNull(params.headerParameter(XOkapiHeaders.TENANT));
    String counterReportId = stringOrNull(params.queryParameter("counterReportId"));
    String providerId = stringOrNull(params.queryParameter("providerId"));

    TenantPgPool pool = TenantPgPool.pool(vertx, tenant);
    return pool.getConnection()
        .compose(sqlConnection -> {
          String qry = "SELECT DISTINCT ON (" + titleEntriesTable(pool) + ".id) * FROM "
              + titleEntriesTable(pool);
          if (counterReportId != null) {
            qry = qry + " INNER JOIN " + titleDataTable(pool)
                + " ON reportTitleId = " + titleEntriesTable(pool) + ".id"
                + " WHERE counterReportId = '" + UUID.fromString(counterReportId) + "'";
          } else if (providerId != null) {
            qry = qry + " INNER JOIN " + titleDataTable(pool)
                + " ON reportTitleId = " + titleEntriesTable(pool) + ".id"
                + " WHERE providerId = '" + UUID.fromString(providerId) + "'";
          }
          return sqlConnection.prepare(qry)
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
                            .put("kbTitleId", row.getUUID(4));
                      }
                      Boolean kbManualMatch = row.getBoolean(5);
                      if (kbManualMatch != null) {
                        response.put("kbManualMatch", kbManualMatch);
                      }
                      ctx.response().write(response.encode());
                    });
                    endStream(stream, ctx, sqlConnection, tx);
                    return Future.succeededFuture();
                  })
              ).onFailure(x -> sqlConnection.close());
        });
  }

  Future<Void> postReportTitles(Vertx vertx, RoutingContext ctx) {
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    String tenant = stringOrNull(params.headerParameter(XOkapiHeaders.TENANT));

    TenantPgPool pool = TenantPgPool.pool(vertx, tenant);
    return pool.getConnection()
        .compose(sqlConnection -> {
          Future<Void> future = Future.succeededFuture();
          final JsonArray titles = ctx.getBodyAsJson().getJsonArray("titles");
          for (int i = 0; i < titles.size(); i++) {
            final JsonObject titleEntry = titles.getJsonObject(i);
            UUID id = UUID.fromString(titleEntry.getString("id"));
            String kbTitleName = titleEntry.getString("kbTitleName");
            UUID kbTitleId = UUID.fromString(titleEntry.getString("kbTitleId"));
            future = future.compose(x ->
                sqlConnection.preparedQuery("UPDATE " + titleEntriesTable(pool)
                    + " SET"
                    + " kbTitleName = $2,"
                    + " kbTitleId = $3,"
                    + " kbManualMatch = TRUE"
                    + " WHERE id = $1")
                    .execute(Tuple.of(id, kbTitleName, kbTitleId))
                    .compose(rowSet -> {
                      if (rowSet.rowCount() == 0) {
                        return Future.failedFuture("title " + id + " matches nothing");
                      }
                      return Future.succeededFuture();
                    }));
          }
          return future.eventually(x -> sqlConnection.close());
        })
        .compose(x -> {
          ctx.response().setStatusCode(204);
          ctx.response().end();
          return Future.succeededFuture();
        });
  }

  Future<Void> getTitleData(Vertx vertx, RoutingContext ctx) {
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    String tenant = stringOrNull(params.headerParameter(XOkapiHeaders.TENANT));

    TenantPgPool pool = TenantPgPool.pool(vertx, tenant);
    return pool.getConnection().compose(sqlConnection -> sqlConnection
        .prepare("SELECT * FROM " + titleDataTable(pool))
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
                    .put("providerId", row.getUUID(3))
                    .put("pubYear", row.getString(4))
                    .put("usageYearMonth", row.getString(5))
                    .put("uniqueAccessCount", row.getInteger(6))
                    .put("totalAccessCount", row.getInteger(7))
                    .put("openAccess", row.getBoolean(8));
                ctx.response().write(obj.encode());
              });
              endStream(stream, ctx, sqlConnection, tx);
              return Future.succeededFuture();
            })
        )
        .onFailure(x -> sqlConnection.close()));
  }

  Future<Void> postFromCounter(Vertx vertx, RoutingContext ctx) {
    return populateCounterReportTitles(vertx, ctx)
        .compose(x -> {
          if (Boolean.TRUE.equals(x)) {
            return getReportTitles(vertx, ctx);
          }
          failHandler(404, ctx, "Not Found");
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
      JsonObject resource = ar.getJsonObject(0);
      UUID titleId = UUID.fromString(resource.getString("id"));
      return Future.succeededFuture(Tuple.of(titleId, resource.getString("name")));
    });
  }

  Future<UUID> upsertTeEntry(TenantPgPool pool, SqlConnection con, RoutingContext ctx,
                             String counterReportTitle, String match) {
    return con.preparedQuery("SELECT id FROM " + titleEntriesTable(pool)
        + " WHERE counterReportTitle = $1")
        .execute(Tuple.of(counterReportTitle))
        .compose(res1 -> {
          if (res1.iterator().hasNext()) {
            return Future.succeededFuture(res1.iterator().next().getUUID(0));
          }
          return ermLookup(ctx, match).compose(erm -> {
            Future<Void> future;
            if (erm == null) {
              future = con.preparedQuery("INSERT INTO " + titleEntriesTable(pool)
                  + "(id, counterReportTitle, matchCriteria,"
                  + " kbManualMatch)"
                  + " VALUES ($1, $2, $3, $4)"
                  + " ON CONFLICT (counterReportTitle) DO NOTHING")
                  .execute(Tuple.tuple(List.of(UUID.randomUUID(), counterReportTitle, match,
                      false))).mapEmpty();
            } else {
              UUID kbTitleId = erm.getUUID(0);
              String kbTitleName = erm.getString(1);
              future = con.preparedQuery("INSERT INTO " + titleEntriesTable(pool)
                  + "(id, counterReportTitle, matchCriteria,"
                  + " kbTitleName, kbTitleId,"
                  + " kbManualMatch)"
                  + " VALUES ($1, $2, $3, $4, $5, $6)"
                  + " ON CONFLICT (counterReportTitle) DO NOTHING")
                  .execute(Tuple.tuple(List.of(UUID.randomUUID(), counterReportTitle, match,
                      kbTitleName, kbTitleId,
                      false))).mapEmpty();
            }
            return future.compose(x ->
                con.preparedQuery("SELECT id FROM " + titleEntriesTable(pool)
                    + " WHERE counterReportTitle = $1")
                    .execute(Tuple.of(counterReportTitle))
                    .map(res2 -> res2.iterator().next().getUUID(0))
            );
          });
        });
  }

  static Future<Void> clearTdEntry(TenantPgPool pool, SqlConnection con, UUID counterReportId) {
    return con.preparedQuery("DELETE FROM " + titleDataTable(pool)
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
                                    UUID counterReportId, UUID providerId, String usageYearMonth,
                                    int totalAccessCount) {
    return con.preparedQuery("INSERT INTO " + titleDataTable(pool)
        + "(id, reportTitleId,"
        + " counterReportId, providerId,"
        + " pubYear, usageYearMonth,"
        + " uniqueAccessCount, totalAccessCount, openAccess)"
        + " VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)")
        .execute(Tuple.tuple(List.of(UUID.randomUUID(), reportTitleId,
            counterReportId, providerId,
            "", usageYearMonth,
            totalAccessCount, totalAccessCount, false)))
        .mapEmpty();
  }

  static int getTotalCount(JsonObject reportItem) {
    int count = 0;
    JsonArray itemPerformances = reportItem.getJsonArray(altKey(reportItem,
        "itemPerformance", "Performance"));
    for (int i = 0; i < itemPerformances.size(); i++) {
      JsonObject itemPerformance = itemPerformances.getJsonObject(i);
      if (itemPerformance != null) {
        JsonArray instances = itemPerformance.getJsonArray(altKey(itemPerformance,
            "instance", "Instance"));
        for (int j = 0; j < instances.size(); j++) {
          JsonObject instance = instances.getJsonObject(j);
          if (instance != null) {
            count += instance.getInteger(altKey(instance, "count", "Count"));
          }
        }
      }
    }
    return count;
  }

  static String getMatch(JsonObject reportItem) {
    JsonArray itemIdentifiers = reportItem.getJsonArray(altKey(reportItem,
        "itemIdentifier", "Item_ID"));
    for (int k = 0; k < itemIdentifiers.size(); k++) {
      JsonObject itemIdentifier = itemIdentifiers.getJsonObject(k);
      if (itemIdentifier != null) {
        String type = itemIdentifier.getString(altKey(itemIdentifier, "type", "Type"));
        String value = itemIdentifier.getString(altKey(itemIdentifier, "value", "Value"));
        if ("ONLINE_ISSN".equals(type) || "Online_ISSN".equals(type)) {
          return value;
        }
      }
    }
    return null;
  }

  static String altKey(JsonObject jsonObject, String... keys) {
    for (String key : keys) {
      if (jsonObject.containsKey(key)) {
        return key;
      }
    }
    return keys[0];
  }

  Future<Void> handleReport(TenantPgPool pool, RoutingContext ctx, JsonObject jsonObject) {
    final UUID counterReportId = UUID.fromString(jsonObject.getString("id"));
    final UUID providerId = UUID.fromString(jsonObject.getString("providerId"));
    final String usageYearMonth = jsonObject.getString("yearMonth");
    final JsonObject reportItem = jsonObject.getJsonObject("reportItem");
    final String match = getMatch(reportItem);
    final String counterReportTitle = reportItem.getString(altKey(reportItem,
        "itemName", "Title"));
    log.debug("handleReport title={} match={}", counterReportTitle, match);
    if (match == null) {
      return Future.succeededFuture();
    }
    return pool.getConnection().compose(con -> con.begin().compose(tx -> {
      Future<Void> future = Future.succeededFuture();
      final int totalAccessCount = getTotalCount(reportItem);
      future = future.compose(x ->
          upsertTeEntry(pool, con, ctx, counterReportTitle, match)
              .compose(reportTitleId -> insertTdEntry(pool, con, reportTitleId, counterReportId,
                  providerId, usageYearMonth, totalAccessCount))
      );
      return future.compose(x -> tx.commit());
    }).eventually(x -> con.close()));
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
    AtomicInteger pathSize = new AtomicInteger(id != null ? 1 : 0);
    JsonObject reportObj = new JsonObject();
    parser.handler(event -> {
      log.debug("event type={}", event.type().name());
      JsonEventType type = event.type();
      if (JsonEventType.END_OBJECT.equals(type)) {
        pathSize.decrementAndGet();
        objectMode.set(false);
        parser.objectEventMode();
      }
      if (JsonEventType.START_OBJECT.equals(type)) {
        pathSize.incrementAndGet();
      }
      if (objectMode.get() && event.isObject()) {
        reportObj.put("reportItem", event.objectValue());
        log.debug("Object value {}", reportObj.encodePrettily());
        futures.add(handleReport(pool, ctx, reportObj));
      } else {
        String f = event.fieldName();
        log.debug("Field = {}", f);
        if (pathSize.get() == 2) { // if inside each top-level of each report
          if ("id".equals(f)) {
            reportObj.put("id", event.stringValue());
            futures.add(clearTdEntry(pool, UUID.fromString(reportObj.getString("id"))));
          }
          if ("providerId".equals(f)) {
            reportObj.put(f, event.stringValue());
          }
          if ("yearMonth".equals(f)) {
            reportObj.put(f, event.stringValue());
          }
        }
        if ("reportItems".equals(f) || "Report_Items".equals(f)) {
          objectMode.set(true);
          parser.objectValueMode();
        }
      }
    });
    parser.exceptionHandler(x -> {
      log.error("GET {} returned bad JSON: {}", uri, x.getMessage(), x);
      promise.tryFail("GET " + uri + " returned bad JSON: " + x.getMessage());
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

  Future<Void> getReportData(Vertx vertx, RoutingContext ctx) {
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    String tenant = stringOrNull(params.headerParameter(XOkapiHeaders.TENANT));

    TenantPgPool pool = TenantPgPool.pool(vertx, tenant);
    return pool.getConnection()
        .compose(sqlConnection ->
            sqlConnection.prepare("SELECT * FROM " + reportDataTable(pool))
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
                            .put("type", row.getString(2))
                            .put("agreementLineId", row.getUUID(4))
                            .put("encumberedCost", row.getNumeric(5))
                            .put("invoicedCost", row.getNumeric(6));
                        UUID titleDataId = row.getUUID(1);
                        if (titleDataId != null) {
                          obj.put("titleDataId", titleDataId);
                        }
                        String counterReportTitle = row.getString(3);
                        if (counterReportTitle != null) {
                          obj.put("counterReportTitle", counterReportTitle);
                        }
                        ctx.response().write(obj.encode());
                      });
                      endStream(stream, ctx, sqlConnection, tx);
                      return Future.succeededFuture();
                    })
                )
                .onFailure(x -> sqlConnection.close())
        );
  }

  Future<Boolean> agreementExists(RoutingContext ctx, UUID agreementId) {
    final String uri = "/erm/sas/" + agreementId;
    return createRequest(webClient, HttpMethod.GET, ctx, uri)
        .send()
        .compose(res -> {
          if (res.statusCode() == 404) {
            return Future.succeededFuture(false);
          }
          if (res.statusCode() != 200) {
            return Future.failedFuture("GET " + uri + " returned status code " + res.statusCode());
          }
          return Future.succeededFuture(true);
        });
  }

  Future<Tuple> lookupTitleFromKbTitle(TenantPgPool pool, UUID kbTitleId) {
    return pool.preparedQuery("SELECT * FROM " + titleEntriesTable(pool)
        + " WHERE kbTitleId = $1")
        .execute(Tuple.of(kbTitleId))
        .compose(res -> {
          if (res.iterator().hasNext()) {
            Row row = res.iterator().next();
            return Future.succeededFuture(Tuple.of(
                row.getUUID(0), row.getString(1)));
          }
          return Future.succeededFuture();
        });
  }

  Future<JsonObject> lookupOrderLine(UUID poLineId, RoutingContext ctx) {
    String uri = "/orders/order-lines/" + poLineId;
    return createRequest(webClient, HttpMethod.GET, ctx, uri)
        .send()
        .compose(res -> {
          if (res.statusCode() != 200) {
            return Future.failedFuture("GET " + uri + " returned status code " + res.statusCode());
          }
          return Future.succeededFuture(res.bodyAsJsonObject());
        });
  }

  Future<JsonArray> lookupInvoiceLine(UUID poLineId, RoutingContext ctx) {
    String uri = "/invoice-storage/invoice-lines?query=poLineId%3D%3D" + poLineId;
    return createRequest(webClient, HttpMethod.GET, ctx, uri)
        .send()
        .compose(res -> {
          if (res.statusCode() != 200) {
            return Future.failedFuture("GET " + uri + " returned status code " + res.statusCode());
          }
          return Future.succeededFuture(res.bodyAsJsonArray());
        });
  }

  Future<JsonObject> lookupPoLine(JsonArray poLinesAr, RoutingContext ctx) {
    List<Future<Void>> futures = new LinkedList<>();

    JsonObject totalCost = new JsonObject();
    totalCost.put("encumberedCost", 0.0);
    totalCost.put("invoicedCost", 0.0);
    for (int i = 0; i < poLinesAr.size(); i++) {
      UUID poLineId = UUID.fromString(poLinesAr.getJsonObject(i).getString("poLineId"));
      futures.add(lookupOrderLine(poLineId, ctx).compose(poLine -> {
        JsonObject cost = poLine.getJsonObject("cost");
        String currency = totalCost.getString("currency");
        String newCurrency = cost.getString("currency");
        if (currency != null && !currency.equals(newCurrency)) {
          return Future.failedFuture("Mixed currencies (" + currency + ", " + newCurrency
              + ") in order lines " + poLinesAr.encode());
        }
        totalCost.put("currency", newCurrency);
        totalCost.put("encumberedCost",
            totalCost.getDouble("encumberedCost") + cost.getDouble("listUnitPriceElectronic"));
        return Future.succeededFuture();
      }));
      futures.add(lookupInvoiceLine(poLineId, ctx).compose(invoices -> {
        for (int j = 0; j < invoices.size(); j++) {
          JsonObject invoiceLine = invoices.getJsonObject(j);
          Double thisTotal = invoiceLine.getDouble("total");
          if (thisTotal != null) {
            totalCost.put("invoicedCost", thisTotal + totalCost.getDouble("invoicedCost"));
          }
        }
        return Future.succeededFuture();
      }));
    }
    return GenericCompositeFuture.all(futures).map(totalCost);
  }

  Future<Void> populateAgreementLine(JsonObject agreementLine, TenantPgPool pool, UUID agreementId,
                                     RoutingContext ctx) {
    JsonObject resourceObject = agreementLine.getJsonObject("resource");
    if (resourceObject == null) {
      return Future.failedFuture("Missing property resource from agreement " + agreementId);
    }
    JsonObject underScoreObject = resourceObject.getJsonObject("_object");
    if (underScoreObject == null) {
      return Future.failedFuture("Missing property _object from agreement " + agreementId);
    }
    JsonObject pti = underScoreObject.getJsonObject("pti");
    if (pti == null) {
      return Future.failedFuture("Missing property pti from agreement " + agreementId);
    }
    JsonObject titleInstance = pti.getJsonObject("titleInstance");
    if (titleInstance == null) {
      return Future.failedFuture("Missing property titleInstance from agreement " + agreementId);
    }
    UUID agreementLineId = UUID.fromString(agreementLine.getString("id"));
    UUID kbTitleId = UUID.fromString(titleInstance.getString("id"));
    String type = titleInstance.getJsonObject("publicationType").getString("value");
    return lookupPoLine(agreementLine.getJsonArray("poLines"), ctx)
        .compose(cost ->
            lookupTitleFromKbTitle(pool, kbTitleId)
                .compose(tuple -> {
                  UUID id = UUID.randomUUID();
                  UUID titleDataId = tuple != null ? tuple.getUUID(0) : null;
                  String counterReportTitle = tuple != null ? tuple.getString(1) : null;
                  Number encumberedCost = cost.getDouble("encumberedCost");
                  Number invoicedCost = cost.getDouble("invoicedCost");
                  return pool.preparedQuery("INSERT INTO " + reportDataTable(pool)
                      + "(id, titleDataId, type, counterReportTitle, agreementLineId,"
                      + " encumberedCost, invoicedCost)"
                      + " VALUES ($1, $2, $3, $4, $5, $6, $7)")
                      .execute(Tuple.of(id, titleDataId, type, counterReportTitle, agreementLineId,
                          encumberedCost, invoicedCost))
                      .mapEmpty();
                })
        );
  }

  Future<Integer> populateAgreement(Vertx vertx, RoutingContext ctx) {
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    final String tenant = stringOrNull(params.headerParameter(XOkapiHeaders.TENANT));
    final String agreementIdStr = ctx.getBodyAsJson().getString("agreementId");
    if (agreementIdStr == null) {
      return Future.failedFuture("Missing agreementId property");
    }
    TenantPgPool pool = TenantPgPool.pool(vertx, tenant);
    final UUID agreementId = UUID.fromString(agreementIdStr);
    return agreementExists(ctx, agreementId)
        .compose(exists -> {
          if (!exists) {
            return Future.succeededFuture(null);
          }
          // expand agreement to get agreement lines, now that we know the agreement ID is good.
          // the call below returns 500 with a stacktrace if agremment ID is no good.
          String uri = "/erm/entitlements?filters=owner%3D" + agreementId;
          return createRequest(webClient, HttpMethod.GET, ctx, uri)
              .send()
              .compose(res -> {
                if (res.statusCode() != 200) {
                  return Future.failedFuture("GET " + uri + " returned status code "
                      + res.statusCode());
                }
                // TODO clear all existing entries from that agreement and run in transaction
                Future<Void> future = Future.succeededFuture();
                JsonArray items = res.bodyAsJsonArray();
                for (int i = 0; i < items.size(); i++) {
                  JsonObject agreementLine = items.getJsonObject(i);
                  future = future
                      .compose(v -> populateAgreementLine(agreementLine, pool, agreementId, ctx));
                }
                return future.map(items.size());
              });
        });
  }

  Future<Integer> postFromAgreement(Vertx vertx, RoutingContext ctx) {
    return populateAgreement(vertx, ctx)
        .compose(linesCreated -> {
          if (linesCreated == null) {
            failHandler(404, ctx, "Not found");
            return Future.succeededFuture();
          }
          ctx.response().setStatusCode(200);
          ctx.response().putHeader("Content-Type", "application/json");
          ctx.response().end(new JsonObject().put("reportLinesCreated", linesCreated).encode());
          return Future.succeededFuture();
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
              .operation("postReportTitles")
              .handler(ctx -> postReportTitles(vertx, ctx)
                  .onFailure(cause -> failHandler(400, ctx, cause)));
          routerBuilder
              .operation("postFromCounter")
              .handler(ctx -> postFromCounter(vertx, ctx)
                  .onFailure(cause -> failHandler(400, ctx, cause)));
          routerBuilder
              .operation("getTitleData")
              .handler(ctx -> getTitleData(vertx, ctx)
                  .onFailure(cause -> failHandler(400, ctx, cause)));
          routerBuilder
              .operation("getReportData")
              .handler(ctx -> getReportData(vertx, ctx)
                  .onFailure(cause -> failHandler(400, ctx, cause)));
          routerBuilder
              .operation("postFromAgreement")
              .handler(ctx -> postFromAgreement(vertx, ctx)
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
        .query("CREATE TABLE IF NOT EXISTS " + titleEntriesTable(pool) + " ( "
            + "id UUID PRIMARY KEY, "
            + "counterReportTitle text UNIQUE, "
            + "matchCriteria text, "
            + "kbTitleName text, "
            + "kbTitleId UUID, "
            + "kbManualMatch boolean"
            + ")")
        .execute().mapEmpty();
    future = future.compose(x -> pool
        .query("CREATE TABLE IF NOT EXISTS " + titleDataTable(pool) + " ( "
            + "id UUID PRIMARY KEY, "
            + "reportTitleId UUID, "
            + "counterReportId UUID, "
            + "providerId UUID, "
            + "pubYear text, "
            + "usageYearMonth text, "
            + "uniqueAccessCount integer, "
            + "totalAccessCount integer, "
            + "openAccess boolean"
            + ")")
        .execute().mapEmpty());
    future = future.compose(x -> pool
        .query("CREATE TABLE IF NOT EXISTS " + reportDataTable(pool) + " ( "
            + "id UUID PRIMARY KEY, "
            + "titleDataId UUID, "
            + "type text, "
            + "counterReportTitle text, "
            + "agreementLineId UUID, "
            + "encumberedCost numeric(20, 8), "
            + "invoicedCost numeric(20, 8)"
            + ")")
        .execute().mapEmpty());
    return future;
  }

  @Override
  public Future<Void> preInit(Vertx vertx, String tenant, JsonObject tenantAttributes) {
    return Future.succeededFuture();
  }
}
